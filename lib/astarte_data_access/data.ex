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

defmodule Astarte.DataAccess.Data do
  require Logger
  alias Astarte.DataAccess.Realms.IndividualProperty
  alias Astarte.DataAccess.Repo
  alias Astarte.DataAccess.XandraUtils
  alias Astarte.Core.CQLUtils
  alias Astarte.Core.Device
  alias Astarte.Core.InterfaceDescriptor
  alias Astarte.Core.Mapping
  alias Ecto.UUID
  import Ecto.Query

  @value_columns [
    :binaryblob_value,
    :binaryblobarray_value,
    :boolean_value,
    :booleanarray_value,
    :datetime_value,
    :datetimearray_value,
    :double_value,
    :doublearray_value,
    :integer_value,
    :integerarray_value,
    :longinteger_value,
    :longintegerarray_value,
    :string_value,
    :stringarray_value
  ]

  @spec fetch_property(
          String.t(),
          Device.device_id(),
          %InterfaceDescriptor{},
          %Mapping{},
          String.t()
        ) :: {:ok, any} | {:error, atom}
  def fetch_property(
        realm,
        device_id,
        %InterfaceDescriptor{storage_type: :multi_interface_individual_properties_dbtable} =
          interface_descriptor,
        mapping,
        path
      )
      when is_binary(device_id) and is_binary(path) do
    keyspace = XandraUtils.realm_name_to_keyspace_name(realm)

    value_column =
      mapping.value_type
      |> CQLUtils.type_to_db_column_name()
      |> String.to_existing_atom()

    device_id = UUID.cast!(device_id)
    interface_id = UUID.cast!(interface_descriptor.interface_id)
    endpoint_id = UUID.cast!(mapping.endpoint_id)

    # select all possible column types, we don't care even if it's nil
    query =
      from interface_descriptor.storage,
        prefix: ^keyspace,
        where: [
          device_id: ^device_id,
          interface_id: ^interface_id,
          endpoint_id: ^endpoint_id,
          path: ^path
        ],
        select: ^@value_columns

    with {:ok, %{^value_column => value}} <-
           Repo.fetch_one(query, consistency: :quorum, error: :property_not_set) do
      case value do
        nil -> {:error, :undefined_property}
        value -> {:ok, value}
      end
    end
  end

  @spec path_exists?(
          String.t(),
          Device.device_id(),
          %InterfaceDescriptor{},
          %Mapping{},
          String.t()
        ) :: {:ok, boolean} | {:error, atom}
  def path_exists?(
        realm,
        device_id,
        interface_descriptor,
        mapping,
        path
      )
      when is_binary(device_id) and is_binary(path) do
    fetch(realm, device_id, interface_descriptor, mapping, path)
    |> Repo.aggregate(:count, consistency: :quorum)
    |> case do
      0 -> {:ok, false}
      1 -> {:ok, true}
    end
  end

  @spec fetch_last_path_update(
          String.t(),
          Device.device_id(),
          %InterfaceDescriptor{},
          %Mapping{},
          String.t()
        ) ::
          {:ok, %{value_timestamp: DateTime.t(), reception_timestamp: DateTime.t()}}
          | {:error, atom}
  def fetch_last_path_update(
        realm,
        device_id,
        interface_descriptor,
        mapping,
        path
      )
      when is_binary(device_id) and is_binary(path) do
    query =
      fetch(realm, device_id, interface_descriptor, mapping, path)
      |> select([:datetime_value, :reception_timestamp, :reception_timestamp_submillis])

    with {:ok, last_update} <- Repo.fetch_one(query, error: :path_not_set) do
      value_timestamp = last_update.datetime_value |> DateTime.truncate(:millisecond)
      reception_timestamp = IndividualProperty.reception(last_update)

      {:ok, %{value_timestamp: value_timestamp, reception_timestamp: reception_timestamp}}
    end
  end

  defp fetch(source \\ IndividualProperty, realm, device_id, interface_descriptor, mapping, path) do
    keyspace = XandraUtils.realm_name_to_keyspace_name(realm)

    from source,
      prefix: ^keyspace,
      where: [
        device_id: ^device_id,
        interface_id: ^interface_descriptor.interface_id,
        endpoint_id: ^mapping.endpoint_id,
        path: ^path
      ]
  end
end
