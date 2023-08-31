#
# This file is part of Astarte.
#
# Copyright 2018 - 2024 SECO Mind Srl
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

defmodule Astarte.DataAccess.Mappings do
  alias Astarte.Core.Mapping
  alias Astarte.DataAccess.Realms.Endpoint
  alias Astarte.DataAccess.XandraUtils
  require Logger

  import Ecto.Query
  alias Astarte.DataAccess.Repo

  @default_selection [
    :endpoint,
    :value_type,
    :reliability,
    :retention,
    :database_retention_policy,
    :database_retention_ttl,
    :expiry,
    :allow_unset,
    :explicit_timestamp,
    :endpoint_id,
    :interface_id
  ]

  @spec fetch_interface_mappings(String.t(), binary, keyword) ::
          {:ok, list(%Mapping{})} | {:error, atom}
  def fetch_interface_mappings(realm, interface_id, opts \\ []) do
    keyspace = XandraUtils.realm_name_to_keyspace_name(realm)

    query =
      from Endpoint,
        prefix: ^keyspace,
        where: [interface_id: ^interface_id]

    query =
      if Keyword.get(opts, :include_docs),
        do: query,
        else: query |> select(^@default_selection)

    Repo.all(query, consistency: :quorum)
    |> Enum.map(&mapping_from_endpoint/1)
    |> case do
      [] -> {:error, :interface_not_found}
      mappings -> {:ok, mappings}
    end
  end

  @spec fetch_interface_mappings_map(String.t(), binary, keyword) :: {:ok, map()} | {:error, atom}
  def fetch_interface_mappings_map(realm_name, interface_id, opts \\ []) do
    with {:ok, mappings_list} <- fetch_interface_mappings(realm_name, interface_id, opts) do
      mappings_map =
        mappings_list
        |> Map.new(&{&1.endpoint_id, &1})

      {:ok, mappings_map}
    end
  end

  defp mapping_from_endpoint(endpoint) do
    %Endpoint{
      endpoint: endpoint,
      value_type: value_type,
      reliability: reliability,
      retention: retention,
      expiry: expiry,
      database_retention_policy: database_retention_policy,
      database_retention_ttl: database_retention_ttl,
      allow_unset: allow_unset,
      explicit_timestamp: explicit_timestamp,
      endpoint_id: endpoint_id,
      interface_id: interface_id,
      doc: doc,
      description: description
    } = endpoint

    # If nil, treat as no_ttl
    database_retention_policy = database_retention_policy || :no_ttl

    %Mapping{
      endpoint: endpoint,
      value_type: value_type,
      reliability: reliability,
      retention: retention,
      expiry: expiry,
      database_retention_policy: database_retention_policy,
      database_retention_ttl: database_retention_ttl,
      allow_unset: allow_unset,
      explicit_timestamp: explicit_timestamp,
      endpoint_id: endpoint_id,
      interface_id: interface_id,
      doc: doc,
      description: description
    }
  end
end
