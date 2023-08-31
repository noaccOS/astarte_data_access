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

defmodule Astarte.DataAccess.Interface do
  require Logger
  alias Astarte.Core.InterfaceDescriptor
  alias Astarte.DataAccess.Realms.Interface
  alias Astarte.DataAccess.Repo
  alias Astarte.DataAccess.XandraUtils
  import Ecto.Query

  @default_selection [
    :name,
    :major_version,
    :minor_version,
    :interface_id,
    :type,
    :ownership,
    :aggregation,
    :storage,
    :storage_type,
    :automaton_transitions,
    :automaton_accepting_states
  ]

  @spec retrieve_interface_row(String.t(), String.t(), integer, keyword()) ::
          {:ok, Interface.t()} | {:error, atom}
  def retrieve_interface_row(realm, interface_name, major_version, opts \\ []) do
    keyspace = XandraUtils.realm_name_to_keyspace_name(realm)

    query =
      from Interface,
        prefix: ^keyspace,
        where: [name: ^interface_name, major_version: ^major_version]

    query =
      if Keyword.get(opts, :include_docs),
        do: query,
        else: query |> select(^@default_selection)

    Repo.fetch_one(query, error: :interface_not_found)
  end

  @spec fetch_interface_descriptor(String.t(), String.t(), non_neg_integer) ::
          {:ok, %InterfaceDescriptor{}} | {:error, atom}
  def fetch_interface_descriptor(realm_name, interface_name, major_version) do
    with {:ok, interface} <- retrieve_interface_row(realm_name, interface_name, major_version) do
      {:ok, interface_descriptor_from_interface(interface)}
    end
  end

  @spec check_if_interface_exists(String.t(), String.t(), non_neg_integer) ::
          :ok | {:error, atom}
  def check_if_interface_exists(realm, interface_name, major_version) do
    keyspace = XandraUtils.realm_name_to_keyspace_name(realm)

    query =
      from Interface,
        prefix: ^keyspace,
        where: [name: ^interface_name, major_version: ^major_version]

    case Repo.aggregate(query, :count) do
      1 -> :ok
      0 -> {:error, :interface_not_found}
    end
  end

  defp interface_descriptor_from_interface(interface) do
    %Interface{
      name: name,
      major_version: major_version,
      minor_version: minor_version,
      type: type,
      ownership: ownership,
      aggregation: aggregation,
      automaton_accepting_states: automaton_accepting_states,
      automaton_transitions: automaton_transitions,
      storage: storage,
      storage_type: storage_type,
      interface_id: interface_id
    } = interface

    %InterfaceDescriptor{
      name: name,
      major_version: major_version,
      minor_version: minor_version,
      type: type,
      ownership: ownership,
      aggregation: aggregation,
      storage: storage,
      storage_type: storage_type,
      automaton:
        {:erlang.binary_to_term(automaton_transitions),
         :erlang.binary_to_term(automaton_accepting_states)},
      interface_id: interface_id
    }
  end
end
