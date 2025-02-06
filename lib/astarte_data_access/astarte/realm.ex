#
# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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

defmodule Astarte.DataAccess.Astarte.Realm do
  use TypedEctoSchema

  alias Astarte.Core.CQLUtils
  alias Astarte.DataAccess.Config
  alias Astarte.DataAccess.Repo
  alias Astarte.DataAccess.Astarte.KvStore
  alias Astarte.DataAccess.Astarte.Realm.RealmCreationOptions

  import Ecto.Query

  @primary_key {:realm_name, :string, autogenerate: false}
  typed_schema "realms" do
    field :device_registration_limit, :integer
  end

  @spec keyspace_name(String.t()) :: String.t()
  def keyspace_name(realm_name) do
    CQLUtils.realm_name_to_keyspace_name(realm_name, Config.astarte_instance_id!())
  end

  @default_realm_schema_version 10
  @default_replication_factor 1

  @spec create(RealmCreationOption.t()) :: :ok
  def create(options) do
    %Realm
    %Realm
    realm_name = options.realm_name
    device_registration_limit = options.device_registration_limit

    keyspace = keyspace_name(realm_name)
    astarte_keyspace = keyspace_name("astarte")

    with :ok <- check_replication(options.replication),
         {:ok, replication_map} <- build_replication_map(options.replication),
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
         :ok <- insert_realm_public_key(keyspace, options.public_key_pem),
         {:ok, _} <- insert_realm_astarte_schema_version(keyspace, options.realm_schema_version),
         {:ok, _} <-
           insert_realm(astarte_keyspace, realm_name, options.device_registration_limit),
         :ok <- insert_datastream_max_retention(keyspace, options.max_retention) do
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

  # Replication factor of 1 is always ok
  defp check_replication(1), do: :ok

  # If replication factor is an integer, we're using SimpleStrategy
  # Check that the replication factor is <= the number of nodes in the same datacenter
  defp check_replication(replication_factor)
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

  defp check_replication(datacenter_replication_factors)
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

  defp build_replication_map(replication_factor)
       when is_integer(replication_factor) and replication_factor > 0 do
    replication_map_str =
      "{'class': 'SimpleStrategy', 'replication_factor': #{replication_factor}}"

    {:ok, replication_map_str}
  end

  defp build_replication_map(datacenter_replication_factors)
       when is_map(datacenter_replication_factors) do
    datacenter_replications_str =
      datacenter_replication_factors
      |> Enum.map(fn {datacenter, replication_factor} ->
        "'#{datacenter}': #{replication_factor}"
      end)
      |> Enum.join(",")

    replication_map_str = "{'class': 'NetworkTopologyStrategy', #{datacenter_replications_str}}"

    {:ok, replication_map_str}
  end

  defp build_replication_map(_invalid_replication), do: {:error, :invalid_replication}

  defp delete_realm_keyspace(keyspace_name) do
    query = """
    DROP KEYSPACE #{keyspace_name}
    """

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_realm_keyspace(keyspace, replication_map_str) do
    query = """
    CREATE KEYSPACE
      #{keyspace}
    WITH
      replication = #{replication_map_str}
    AND
      durable_writes = true
    """

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_realm_kv_store(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.kv_store (
      group varchar,
      key varchar,
      value blob,
      PRIMARY KEY ((group), key)
    )
    """

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_names_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.names (
      object_name varchar,
      object_type int,
      object_uuid uuid,
      PRIMARY KEY ((object_name), object_type)
    )
    """

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_devices_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.devices (
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

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_endpoints_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.endpoints (
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

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_interfaces_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.interfaces (
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

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_individual_properties_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.individual_properties (
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

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_simple_triggers_table(keyspace_name) do
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

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_grouped_devices_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.grouped_devices (
      group_name varchar,
      insertion_uuid timeuuid,
      device_id uuid,
      PRIMARY KEY (
        (group_name), insertion_uuid, device_id
      )
    )
    """

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp create_deletion_in_progress_table(keyspace) do
    query = """
    CREATE TABLE #{keyspace}.deletion_in_progress (
      device_id uuid,
      vmq_ack boolean,
      dup_start_ack boolean,
      dup_end_ack boolean,
      PRIMARY KEY (device_id)
    )
    """

    with {:ok, %Xandra.SchemaChange{}} <- CSystem.execute_schema_change(query) do
      :ok
    end
  end

  defp insert_realm_public_key(keyspace, public_key_pem) do
    query = """
    INSERT INTO #{keyspace}.kv_store (
      group,
      key,
      value
    )
    VALUES (
      'auth',
      'jwt_public_key_pem',
      varcharAsBlob(:public_key_pem)
    )
    """

    params = %{
      public_key_pem: public_key_pem
    }

    with {:ok, _} <- XandraUtils.execute_query(query, params, consistency: :each_quorum) do
      :ok
    end
  end

  defp insert_realm_astarte_schema_version(keyspace, realm_schema_version) do
    query = """
    INSERT INTO #{keyspace}.kv_store (
      group,
      key,
      value
    )
    VALUES (
      'astarte',
      'schema_version',
      bigintAsBlob(:realm_schema_version)
    )
    """

    params = %{"realm_schema_version" => realm_schema_version}

    Ecto.Adapters.SQL.query(Repo, query, params)
  end

  defp insert_realm(astarte_keyspace, realm_name, device_registration_limit) do
    realm = %__MODULE__{
      realm_name: realm_name,
      device_registration_limit: device_registration_limit
    }

    Repo.insert(realm, prefix: astarte_keyspace, consistency: :each_quorum)
  end

  # ScyllaDB considers TTL=0 as unset, see
  # https://opensource.docs.scylladb.com/stable/cql/time-to-live.html#notes
  defp insert_datastream_max_retention(_keyspace_name, 0), do: :ok

  defp insert_datastream_max_retention(keyspace_name, max_retention) do
    query = """
    INSERT INTO #{keyspace_name}.kv_store (
      group,
      key,
      value
    )
    VALUES (
      'realm_config',
      'datastream_maximum_storage_retention',
      intAsBlob(:max_retention)
    )
    """

    params = %{
      "max_retention" => max_retention
    }

    Ecto.Adapters.SQL.query(Repo, query, params, consistency: :each_quorum)
  end
end
