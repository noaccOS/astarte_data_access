#
# This file is part of Astarte.
#
# Copyright 2024 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

defmodule Astarte.DataAccess.Realm do
  require Logger

  alias Astarte.DataAccess.Astarte.KvStore
  alias Astarte.DataAccess.Astarte.Realm
  alias Astarte.DataAccess.CSystem
  alias Astarte.DataAccess.Repo
  alias Astarte.DataAccess.XandraUtils

  import Ecto.Query

  def create_realm(
        realm_name,
        max_retention,
        public_key_pem,
        replication \\ @default_replication_factor,
        device_registration_limit \\ nil,
        realm_schema_version \\ @default_realm_schema_version
      ) do
    keyspace = Realm.keyspace_name(realm_name)
    astarte_keyspace = Realm.keyspace_name("astarte")

    with :ok <- check_replication(replication),
         {:ok, replication_map} <- build_replication_map_str(replication),
         :ok <- create_realm_keyspace(keyspace, replication_map),
         :ok <- create_realm_kv_store(keyspace),
         :ok <- create_names_table(keyspace),
         :ok <- create_devices_table(keyspace),
         :ok <- create_endpoints_table(keyspace),
         :ok <- create_interfaces_table(keyspace),
         :ok <- create_individual_properties_table(keyspace),
         :ok <- create_simple_triggers_table(keyspace),
         :ok <- create_grouped_devices_table(keyspace),
         :ok <- create_deletion_in_progress_table(keyspace),
         :ok <- insert_realm_public_key(keyspace, public_key_pem),
         :ok <- insert_realm_astarte_schema_version(keyspace, realm_schema_version),
         {:ok, _} <- insert_realm(astarte_keyspace, realm_name, device_registration_limit),
         :ok <- insert_datastream_max_retention(keyspace, max_retention) do
      :ok
    else
      {:error, reason} ->
        Logger.warning("Cannot create realm: #{inspect(reason)}.",
          tag: "realm_creation_failed",
          realm: realm_name
        )

        {:error, reason}
    end
  end

  def delete_realm(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)
    astarte_keyspace = Realm.keyspace_name("astarte")

    delete_realm_entry =
      from Realm,
        where: [realm_name: ^realm_name]

    with :ok <- check_no_connected_devices(keyspace),
         :ok <- delete_realm_keyspace(keyspace) do
      Repo.delete_all(delete_realm_entry, prefix: astarte_keyspace, consistency: :each_quorum)

      :ok
    else
      {:error, reason} ->
        Logger.warning("Cannot delete realm: #{inspect(reason)}.",
          tag: "realm_deletion_failed",
          realm: realm_name
        )

        {:error, reason}
    end
  end

  def exists?(realm_name) do
    astarte_keyspace = Realm.keyspace_name("astarte")

    case Repo.fetch(Realm, realm_name, prefix: astarte_keyspace, consistency: :quorum) do
      {:ok, _realm} -> true
      {:error, :not_found} -> false
    end
  end

  def list_realms() do
    astarte_keyspace = Realm.keyspace_name("astarte")

    query = from r in Realm, prefix: ^astarte_keyspace, select: r.realm_name
    Repo.all(query, consistency: :quorum)
  end

  def get_realm(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)

    with {:ok, public_key} <- get_public_key(keyspace),
         {:ok, replication_map} <- get_realm_replication(keyspace),
         {:ok, device_registration_limit} <-
           get_device_registration_limit(realm_name) do
      max_retention =
        get_datastream_maximum_storage_retention(keyspace)

      case replication_map do
        %{
          class: "org.apache.cassandra.locator.SimpleStrategy",
          replication_factor: replication_factor_string
        } ->
          replication_factor = Integer.parse(replication_factor_string)

          %{
            realm_name: realm_name,
            jwt_public_key_pem: public_key,
            replication_class: "SimpleStrategy",
            replication_factor: replication_factor,
            device_registration_limit: device_registration_limit,
            datastream_maximum_storage_retention: max_retention
          }

        %{class: "org.apache.cassandra.locator.NetworkTopologyStrategy"} ->
          datacenter_replication_factors =
            replication_map
            |> Map.drop(["class"])
            |> Map.new(fn {datacenter, replication_factor} ->
              {datacenter, String.to_integer(replication_factor)}
            end)

          %{
            realm_name: realm_name,
            jwt_public_key_pem: public_key,
            replication_class: "NetworkTopologyStrategy",
            datacenter_replication_factors: datacenter_replication_factors,
            device_registration_limit: device_registration_limit,
            datastream_maximum_storage_retention: max_retention
          }
      end
    else
      {:error, reason} ->
        Logger.warning("Error while getting realm: #{inspect(reason)}.",
          tag: "get_realm_error",
          realm: realm_name
        )

        {:error, reason}
    end
  end

  def update_public_key(realm_name, new_public_key_pem) do
    case XandraUtils.run(realm_name, fn conn, keyspace_name ->
           do_update_public_key(conn, keyspace_name, new_public_key_pem)
         end) do
      {:ok, _result} ->
        :ok

      {:error, reason} ->
        Logger.warning("Cannot update public key: #{inspect(reason)}.",
          tag: "realm_updating_public_key",
          realm: realm_name
        )

        {:error, reason}
    end
  end

  # Replication factor of 1 is always ok
  def check_replication(1), do: :ok

  # If replication factor is an integer, we're using SimpleStrategy
  # Check that the replication factor is <= the number of nodes in the same datacenter
  def check_replication(replication_factor)
      when is_integer(replication_factor) and replication_factor > 1 do
    local_datacenter =
      from(l in "local", select: l.data_center)
      |> Repo.one!(prefix: "system")

    local_datacenter_node_count =
      from(p in "peers",
        prefix: "system",
        hints: ["ALLOW FILTERING"],
        where: p.data_center == ^local_datacenter
      )
      |> Repo.aggregate(:count)

    # +1 because the local datacenter is not counted
    node_count_by_datacenter = %{local_datacenter => local_datacenter_node_count + 1}

    check_replication_factor(node_count_by_datacenter, local_datacenter, replication_factor)
  end

  def check_replication(datacenter_replication_factors)
      when is_map(datacenter_replication_factors) do
    node_count_by_datacenter = node_count_by_datacenter()

    datacenter_replication_factors
    |> Stream.map(fn {data_center, replication_factor} ->
      check_replication_factor(node_count_by_datacenter, data_center, replication_factor)
    end)
    |> Enum.find(:ok, &(&1 != :ok))
  end

  defp node_count_by_datacenter do
    local_datacenter =
      from(l in "local", select: l.data_center)
      |> Repo.one!(prefix: "system")

    from(p in "peers", prefix: "system", select: p.data_center)
    |> Repo.all()
    |> Enum.frequencies()
    |> Map.update(local_datacenter, 1, &(&1 + 1))
  end

  defp check_replication_factor(node_count_by_datacenter, data_center, replication_factor) do
    case Map.fetch(node_count_by_datacenter, data_center) do
      {:ok, node_count} when node_count >= replication_factor ->
        :ok

      {:ok, node_count} ->
        Logger.warning(
          "Trying to set replication_factor #{replication_factor} " <>
            "in data_center #{data_center} that has #{node_count} nodes.",
          tag: "invalid_replication_factor",
          data_center: data_center,
          replication_factor: replication_factor
        )

        error_message =
          "replication_factor #{replication_factor} is >= #{node_count} nodes " <>
            "in data_center #{data_center}"

        {:error, {:invalid_replication, error_message}}

      :error ->
        Logger.warning("Cannot retrieve node count for datacenter #{data_center}.",
          tag: "datacenter_not_found",
          data_center: data_center
        )

        {:error, :datacenter_not_found}
    end
  end

  defp build_replication_map_str(replication_factor)
       when is_integer(replication_factor) and replication_factor > 0 do
    replication_map_str =
      "{'class': 'SimpleStrategy', 'replication_factor': #{replication_factor}}"

    {:ok, replication_map_str}
  end

  defp build_replication_map_str(datacenter_replication_factors)
       when is_map(datacenter_replication_factors) do
    datacenter_replications_str =
      Enum.map(datacenter_replication_factors, fn {datacenter, replication_factor} ->
        "'#{datacenter}': #{replication_factor}"
      end)
      |> Enum.join(",")

    replication_map_str = "{'class': 'NetworkTopologyStrategy', #{datacenter_replications_str}}"

    {:ok, replication_map_str}
  end

  defp build_replication_map_str(_invalid_replication), do: {:error, :invalid_replication}

  defp check_no_connected_devices(keyspace) do
    query =
      from(Device,
        hints: ["ALLOW FILTERING"],
        where: [connected: true],
        limit: 1,
        select: [:device_id]
      )

    case Repo.fetch_one(query, prefix: keyspace) do
      {:error, :not_found} ->
        :ok

      {:ok, _} ->
        Logger.warning("Realm #{keyspace} still has connected devices.",
          tag: "connected_devices_present"
        )

        Logger.warning("Realm deletion preconditions are not satisfied: #{inspect(reason)}.",
          tag: "realm_deletion_preconditions_rejected",
          realm: keyspace
        )

        {:error, :connected_devices_present}
    end
  end

  defp delete_realm_keyspace(keyspace) do
    with {:ok, _} <- CSystem.execute_schema_change("DROP KEYSPACE #{keyspace}") do
      :ok
    end
  end

  defp create_realm_keyspace(realm_name, replication_map_str) do
    # TODO: use Ecto migrations
    query = """
    CREATE KEYSPACE
      #{realm_name}
    WITH
      replication = #{replication_map_str}
    AND
      durable_writes = true
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_realm_kv_store(realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.kv_store (
      group varchar,
      key varchar,
      value blob,
      PRIMARY KEY ((group), key)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_names_table(realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.names (
      object_name varchar,
      object_type int,
      object_uuid uuid,
      PRIMARY KEY ((object_name), object_type)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_devices_table(realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.devices (
      device_id uuid,
      aliases map<ascii, varchar>,
      introspection map<ascii, int>,
      introspection_minor map<ascii, int>,
      old_introspection map<frozen<tuple<ascii, int>>, int>,
      protocol_revision int,
      first_registration timestamp,
      credentials_secret ascii,
      inhibit_credentials_request boolean,
      cert_serial ascii,
      cert_aki ascii,
      first_credentials_request timestamp,
      last_connection timestamp,
      last_disconnection timestamp,
      connected boolean,
      pending_empty_cache boolean,
      total_received_msgs bigint,
      total_received_bytes bigint,
      exchanged_bytes_by_interface map<frozen<tuple<ascii, int>>, bigint>,
      exchanged_msgs_by_interface map<frozen<tuple<ascii, int>>, bigint>,
      last_credentials_request_ip inet,
      last_seen_ip inet,
      attributes map<varchar, varchar>,

      groups map<text, timeuuid>,

      PRIMARY KEY (device_id)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_endpoints_table(conn, realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.endpoints (
      interface_id uuid,
      endpoint_id uuid,
      interface_name ascii,
      interface_major_version int,
      interface_minor_version int,
      interface_type int,
      endpoint ascii,
      value_type int,
      reliability int,
      retention int,
      expiry int,
      database_retention_ttl int,
      database_retention_policy int,
      allow_unset boolean,
      explicit_timestamp boolean,
      description varchar,
      doc varchar,

      PRIMARY KEY ((interface_id), endpoint_id)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_interfaces_table(conn, realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.interfaces (
      name ascii,
      major_version int,
      minor_version int,
      interface_id uuid,
      storage_type int,
      storage ascii,
      type int,
      ownership int,
      aggregation int,
      automaton_transitions blob,
      automaton_accepting_states blob,
      description varchar,
      doc varchar,

      PRIMARY KEY (name, major_version)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_individual_properties_table(conn, realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.individual_properties (
      device_id uuid,
      interface_id uuid,
      endpoint_id uuid,
      path varchar,
      reception_timestamp timestamp,
      reception_timestamp_submillis smallint,

      double_value double,
      integer_value int,
      boolean_value boolean,
      longinteger_value bigint,
      string_value varchar,
      binaryblob_value blob,
      datetime_value timestamp,
      doublearray_value list<double>,
      integerarray_value list<int>,
      booleanarray_value list<boolean>,
      longintegerarray_value list<bigint>,
      stringarray_value list<varchar>,
      binaryblobarray_value list<blob>,
      datetimearray_value list<timestamp>,

      PRIMARY KEY((device_id, interface_id), endpoint_id, path)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_simple_triggers_table(conn, keyspace_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{keyspace_name}.simple_triggers (
      object_id uuid,
      object_type int,
      parent_trigger_id uuid,
      simple_trigger_id uuid,
      trigger_data blob,
      trigger_target blob,

      PRIMARY KEY ((object_id, object_type), parent_trigger_id, simple_trigger_id)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_grouped_devices_table(conn, realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.grouped_devices (
      group_name varchar,
      insertion_uuid timeuuid,
      device_id uuid,
      PRIMARY KEY (
        (group_name), insertion_uuid, device_id
      )
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_deletion_in_progress_table(conn, realm_name) do
    # TODO: use Ecto migrations
    query = """
    CREATE TABLE #{realm_name}.deletion_in_progress (
      device_id uuid,
      vmq_ack boolean,
      dup_start_ack boolean,
      dup_end_ack boolean,
      PRIMARY KEY (device_id)
    )
    """

    with {:ok, _} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp insert_realm_public_key(keyspace, public_key_pem) do
    value = %{
      group: "auth",
      key: "jwt_public_key_pem",
      value: dynamic([_kv], fragment("varcharAsBlob(?)", ^public_key_pem))
    }

    KvStore.insert_with_query(value, prefix: keyspace, consistency: :each_quorum)
  end

  defp insert_realm_astarte_schema_version(keyspace, realm_schema_version) do
    value = %{
      group: "astarte",
      key: "schema_version",
      value: dynamic([_kv], fragment("bigintAsBlob(?)", ^realm_schema_version))
    }

    KvStore.insert_with_query(value, prefix: keyspace, consistency: :each_quorum)
  end

  defp remove_realm(conn, astarte_keyspace_name, realm_name) do
    query = """
    DELETE FROM
      #{astarte_keyspace_name}.realms
    WHERE
      realm_name = :realm_name
    """

    params = %{
      realm_name: realm_name
    }

    with {:ok, _} <- XandraUtils.execute_query(conn, query, params, consistency: :each_quorum) do
      :ok
    end
  end

  defp insert_realm(astarte_keyspace, realm_name, device_registration_limit) do
    realm = %Realm{
      realm_name: realm_name,
      device_registration_limit: device_registration_limit
    }

    Repo.insert(realm, prefix: astarte_keyspace, consistency: :each_quorum)
  end

  # ScyllaDB considers TTL=0 as unset, see
  # https://opensource.docs.scylladb.com/stable/cql/time-to-live.html#notes
  defp insert_datastream_max_retention(_conn, _keyspace_name, 0), do: :ok

  defp insert_datastream_max_retention(keyspace, max_retention) do
    value = %{
      group: "realm_config",
      key: "datastream_maximum_storage_retention",
      value: dynamic([_kv], fragment("intAsBlob(?)", ^max_retention))
    }

    KvStore.insert_with_query(value, prefix: keyspace, consistency: :each_quorum)
  end

  defp get_public_key(keyspace) do
    query = from kv in KvStore, prefix: ^keyspace, select: fragment("blobAsVarchar(?)", kv.value)
    pk_clause = [group: "auth", key: "jwt_public_key_pem"]

    Repo.fetch_by(query, pk_clause, consistency: :quorum, error: :public_key_not_found)
  end

  defp do_update_public_key(conn, keyspace_name, new_public_key) do
    query = """
    INSERT INTO #{keyspace_name}.kv_store (
      group,
      key,
      value
    )
    VALUES (
      'auth',
      'jwt_public_key_pem',
      varcharAsBlob(:new_public_key)
    )
    """

    params = %{
      new_public_key: new_public_key
    }

    with {:ok, _} <- XandraUtils.execute_query(conn, query, params, consistency: :quorum) do
      :ok
    end
  end

  defp get_realm_replication(keyspace) do
    query =
      from k in "keyspaces",
        prefix: "system_schema",
        select: k.replication

    with {:error, :not_found} <- Repo.fetch_by(query, keyspace_name: keyspace) do
      Logger.error("Cannot find realm replication.",
        tag: "realm_replication_not_found",
        keyspace: keyspace
      )

      {:error, :realm_replication_not_found}
    end
  end

  defp get_device_registration_limit(realm_name) do
    astarte_keyspace = Realm.keyspace_name("astarte")

    query =
      from r in Realm,
        prefix: ^astarte_keyspace,
        select: r.device_registration_limit

    with {:error, :not_found} <- Repo.fetch(query, realm_name) do
      # Something really wrong here, but we still cover this
      Logger.error("Cannot find realm device_registration_limit.",
        tag: "realm_device_registration_limit_not_found",
        realm: realm_name
      )

      {:error, :realm_device_registration_limit_not_found}
    end
  end

  defp get_datastream_maximum_storage_retention(keyspace) do
    query = from kv in KvStore, prefix: ^keyspace, select: fragment("blobAsInt(?)", kv.value)
    pk_clause = [group: "realm_config", key: "datastream_maximum_storage_retention"]

    Repo.get_by(query, pk_clause)
  end
end
