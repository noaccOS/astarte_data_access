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

  alias Astarte.DataAccess.CSystem
  alias Astarte.DataAccess.XandraUtils

  @default_realm_schema_version 10
  @default_replication_factor 1

  def create_realm(
        realm_name,
        max_retention,
        public_key_pem,
        replication \\ @default_replication_factor,
        device_registration_limit \\ nil,
        realm_schema_version \\ @default_realm_schema_version
      ) do
    case XandraUtils.run(realm_name, fn conn, keyspace_name ->
           astarte_keyspace_name = XandraUtils.build_keyspace_name!("astarte")

           with :ok <- check_replication(conn, replication),
                {:ok, replication_map_str} <- build_replication_map_str(replication) do
             do_create_realm(
               conn,
               keyspace_name,
               astarte_keyspace_name,
               realm_name,
               public_key_pem,
               replication_map_str,
               max_retention,
               device_registration_limit,
               realm_schema_version
             )
           end
         end) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Cannot create realm: #{inspect(reason)}.",
          tag: "realm_creation_failed",
          realm: realm_name
        )

        {:error, reason}
    end
  end

  def delete_realm(realm_name) do
    case XandraUtils.run(realm_name, fn conn, keyspace_name ->
           astarte_keyspace_name = XandraUtils.build_keyspace_name!("astarte")
           do_delete_realm(conn, keyspace_name, astarte_keyspace_name, realm_name)
         end) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Cannot delete realm: #{inspect(reason)}.",
          tag: "realm_deletion_failed",
          realm: realm_name
        )

        {:error, reason}
    end
  end

  def is_realm_existing(realm_name) do
    case XandraUtils.run_without_realm_validation("astarte", fn conn, keyspace_name ->
           do_is_realm_existing?(conn, keyspace_name, realm_name)
         end) do
      result when is_boolean(result) -> {:ok, result}
    end
  end

  def list_realms() do
    case XandraUtils.run_without_realm_validation("astarte", fn conn, keyspace_name ->
           do_list_realms(conn, keyspace_name)
         end) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        Logger.warning("Error while listing realms: #{inspect(reason)}.",
          tag: "get_list_realm"
        )

        {:error, reason}
    end
  end

  def get_realm(realm_name) do
    case XandraUtils.run(realm_name, fn conn, keyspace_name ->
           astarte_keyspace_name = XandraUtils.build_keyspace_name!("astarte")
           do_get_realm(conn, keyspace_name, astarte_keyspace_name, realm_name)
         end) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        _ =
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

  defp do_create_realm(
         conn,
         keyspace_name,
         astarte_keyspace_name,
         realm_name,
         public_key_pem,
         replication_map_str,
         max_retention,
         device_registration_limit,
         realm_schema_version
       ) do
    with :ok <- create_realm_keyspace(conn, keyspace_name, replication_map_str),
         :ok <- create_realm_kv_store(conn, keyspace_name),
         :ok <- create_names_table(conn, keyspace_name),
         :ok <- create_devices_table(conn, keyspace_name),
         :ok <- create_endpoints_table(conn, keyspace_name),
         :ok <- create_interfaces_table(conn, keyspace_name),
         :ok <- create_individual_properties_table(conn, keyspace_name),
         :ok <- create_simple_triggers_table(conn, keyspace_name),
         :ok <- create_grouped_devices_table(conn, keyspace_name),
         :ok <- create_deletion_in_progress_table(conn, keyspace_name),
         :ok <- insert_realm_public_key(conn, keyspace_name, public_key_pem),
         :ok <- insert_realm_astarte_schema_version(conn, keyspace_name, realm_schema_version),
         :ok <-
           insert_realm(conn, astarte_keyspace_name, realm_name, device_registration_limit),
         :ok <- insert_datastream_max_retention(conn, keyspace_name, max_retention) do
      :ok
    end
  end

  defp do_list_realms(conn, astarte_keyspace_name) do
    query = """
    SELECT
      realm_name
    FROM
      #{astarte_keyspace_name}.realms
    """

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query, consistency: :quorum),
         list = Enum.map(page, fn %{realm_name: realm_name} -> realm_name end) do
      {:ok, list}
    end
  end

  defp do_get_realm(conn, keyspace_name, astarte_keyspace_name, realm_name) do
    with {:ok, public_key} <- get_public_key(conn, keyspace_name),
         {:ok, replication_map} <- get_realm_replication(conn, keyspace_name),
         {:ok, device_registration_limit} <-
           get_device_registration_limit(conn, astarte_keyspace_name, realm_name),
         {:ok, max_retention} <-
           get_datastream_maximum_storage_retention(conn, keyspace_name) do
      case replication_map do
        %{
          class: "org.apache.cassandra.locator.SimpleStrategy",
          replication_factor: replication_factor_string
        } ->
          {replication_factor, ""} = Integer.parse(replication_factor_string)

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
            Enum.reduce(replication_map, %{}, fn
              {"class", _}, acc ->
                acc

              {datacenter, replication_factor_string}, acc ->
                {replication_factor, ""} = Integer.parse(replication_factor_string)
                Map.put(acc, datacenter, replication_factor)
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
    end
  end

  defp do_delete_realm(conn, keyspace_name, astarte_keyspace_name, realm_name) do
    with :ok <- verify_realm_deletion_preconditions(conn, keyspace_name),
         :ok <- execute_realm_deletion(conn, keyspace_name, astarte_keyspace_name, realm_name) do
      :ok
    end
  end

  # Replication factor of 1 is always ok
  def check_replication(_conn, 1), do: :ok

  # If replication factor is an integer, we're using SimpleStrategy
  # Check that the replication factor is <= the number of nodes in the same datacenter
  def check_replication(conn, replication_factor)
      when is_integer(replication_factor) and replication_factor > 1 do
    with {:ok, local_datacenter} <- get_local_datacenter(conn) do
      check_replication_for_datacenter(conn, local_datacenter, replication_factor, local: true)
    end
  end

  def check_replication(conn, datacenter_replication_factors)
      when is_map(datacenter_replication_factors) do
    with {:ok, local_datacenter} <- get_local_datacenter(conn) do
      Enum.reduce_while(datacenter_replication_factors, :ok, fn
        {datacenter, replication_factor}, _acc ->
          opts =
            if datacenter == local_datacenter do
              [local: true]
            else
              []
            end

          case check_replication_for_datacenter(conn, datacenter, replication_factor, opts) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
      end)
    end
  end

  defp check_replication_for_datacenter(conn, data_center, replication_factor, opts) do
    query = """
    SELECT
      COUNT(*)
    FROM
      system.peers
    WHERE
      data_center = :data_center
    ALLOW FILTERING
    """

    params = %{
      data_center: data_center
    }

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query, params, consistency: :quorum),
         {:ok, %{count: dc_node_count}} <- Enum.fetch(page, 0) do
      # If we're querying the datacenter of the local node, add 1 (itself) to the count
      actual_node_count = if opts[:local], do: dc_node_count + 1, else: dc_node_count

      if replication_factor <= actual_node_count do
        :ok
      else
        _ =
          Logger.warning(
            "Trying to set replication_factor #{replication_factor} " <>
              "in data_center #{data_center} that has #{actual_node_count} nodes.",
            tag: "invalid_replication_factor",
            data_center: data_center,
            replication_factor: replication_factor
          )

        error_message =
          "replication_factor #{replication_factor} is >= #{actual_node_count} nodes " <>
            "in data_center #{data_center}"

        {:error, {:invalid_replication, error_message}}
      end
    else
      :error ->
        _ =
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

  defp verify_realm_deletion_preconditions(conn, keyspace_name) do
    with :ok <- check_no_connected_devices(conn, keyspace_name) do
      :ok
    else
      {:error, reason} ->
        _ =
          Logger.warning("Realm deletion preconditions are not satisfied: #{inspect(reason)}.",
            tag: "realm_deletion_preconditions_rejected",
            realm: keyspace_name
          )

        {:error, reason}
    end
  end

  defp execute_realm_deletion(conn, keyspace_name, astarte_keyspace_name, realm_name) do
    with :ok <- delete_realm_keyspace(conn, keyspace_name),
         :ok <- remove_realm(conn, astarte_keyspace_name, realm_name) do
      :ok
    end
  end

  defp check_no_connected_devices(conn, keyspace_name) do
    query = """
    SELECT
      COUNT(*)
    FROM
      #{keyspace_name}.devices
    WHERE
      connected = true
    LIMIT 1
    ALLOW FILTERING
    """

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query, consistency: :one),
         {:ok, %{count: count}} = Enum.fetch(page, 0),
         true <- count === 0 do
      :ok
    else
      false ->
        _ =
          Logger.warning("Realm #{keyspace_name} still has connected devices.",
            tag: "connected_devices_present"
          )

        {:error, :connected_devices_present}
    end
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

  defp insert_realm(conn, astarte_keyspace_name, realm_name, device_registration_limit) do
    query = """
    INSERT INTO #{astarte_keyspace_name}.realms (
      realm_name,
      device_registration_limit
    )
    VALUES (
      :realm_name,
      :device_registration_limit
    )
    """

    params = %{
      realm_name: realm_name,
      device_registration_limit: device_registration_limit
    }

    with {:ok, prepared} <- Xandra.prepare(conn, query),
         {:ok, %Xandra.Void{}} <-
           Xandra.execute(conn, prepared, params, consistency: :each_quorum) do
      :ok
    end
  end

  # ScyllaDB considers TTL=0 as unset, see
  # https://opensource.docs.scylladb.com/stable/cql/time-to-live.html#notes
  defp insert_datastream_max_retention(_conn, _keyspace_name, 0), do: :ok

  defp insert_datastream_max_retention(conn, keyspace_name, max_retention) do
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
      max_retention: max_retention
    }

    with {:ok, _} <- XandraUtils.execute_query(conn, query, params, consistency: :each_quorum) do
      :ok
    end
  end

  defp do_is_realm_existing?(conn, astarte_keyspace_name, realm_name) do
    query = """
    SELECT
      COUNT(*)
    FROM
      #{astarte_keyspace_name}.realms
    WHERE
      realm_name = :realm_name
    """

    params = %{
      realm_name: realm_name
    }

    with {:ok, prepared} <- Xandra.prepare(conn, query),
         {:ok, page} <-
           Xandra.execute(conn, prepared, params, consistency: :quorum) do
      {:ok, %{count: count}} = Enum.fetch(page, 0)
      not (count === 0)
    end
  end

  defp get_public_key(conn, keyspace_name) do
    query = """
    SELECT
      blobAsVarchar(value)
    FROM
      #{keyspace_name}.kv_store
    WHERE
      group = 'auth'
    AND
      key = 'jwt_public_key_pem';
    """

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query, consistency: :quorum),
         {:ok, %{"system.blobasvarchar(value)": public_key}} <- Enum.fetch(page, 0) do
      {:ok, public_key}
    else
      :error ->
        {:error, :public_key_not_found}
    end
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

  defp get_realm_replication(conn, keyspace_name) do
    query = """
    SELECT
      replication
    FROM
      system_schema.keyspaces
    WHERE
      keyspace_name = :keyspace_name
    """

    params = %{
      keyspace_name: keyspace_name
    }

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query, params),
         {:ok, %{replication: replication_map}} <- Enum.fetch(page, 0) do
      {:ok, replication_map}
    else
      :error ->
        _ =
          Logger.error("Cannot find realm replication.",
            tag: "realm_replication_not_found",
            keyspace: keyspace_name
          )

        {:error, :realm_replication_not_found}
    end
  end

  defp get_device_registration_limit(conn, astarte_keyspace_name, realm_name) do
    query = """
    SELECT
      device_registration_limit
    FROM
      #{astarte_keyspace_name}.realms
    WHERE
      realm_name = :realm_name
    """

    params = %{
      realm_name: realm_name
    }

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query, params),
         {:ok, %{device_registration_limit: value}} <- Enum.fetch(page, 0) do
      {:ok, value}
    else
      :error ->
        # Something really wrong here, but we still cover this
        _ =
          Logger.error("Cannot find realm device_registration_limit.",
            tag: "realm_device_registration_limit_not_found",
            realm: realm_name
          )

        {:error, :realm_device_registration_limit_not_found}
    end
  end

  defp get_datastream_maximum_storage_retention(conn, keyspace_name) do
    query = """
    SELECT
      blobAsInt(value)
    FROM
      #{keyspace_name}.kv_store
    WHERE
      group = 'realm_config'
    AND
      key = 'datastream_maximum_storage_retention'
    """

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query),
         {:ok, %{"system.blobasint(value)": value}} <- Enum.fetch(page, 0) do
      {:ok, value}
    else
      :error ->
        {:ok, nil}
    end
  end

  defp get_local_datacenter(conn) do
    query = """
    SELECT
      data_center
    FROM
      system.local
    """

    with {:ok, page} <- XandraUtils.retrieve_page(conn, query),
         {:ok, %{data_center: datacenter}} <- Enum.fetch(page, 0) do
      {:ok, datacenter}
    else
      :error ->
        _ =
          Logger.error(
            "Empty dataset while getting local datacenter, something is really wrong.",
            tag: "get_local_datacenter_error"
          )

        {:error, :local_datacenter_not_found}
    end
  end
end
