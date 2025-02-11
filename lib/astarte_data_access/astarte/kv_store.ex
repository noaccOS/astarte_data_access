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

defmodule Astarte.DataAccess.Astarte.KvStore do
  use TypedEctoSchema

  alias Astarte.DataAccess.Repo

  import Ecto.Query

  @type value_type :: :integer | :big_integer | :string

  @primary_key false
  typed_schema "kv_store" do
    field :group, :string, primary_key: true
    field :key, :string, primary_key: true
    field :value, :binary
  end

  @doc """
    Insert a KvStore, allowing type conversion at the database level.

    The Ecto mindset is to finalize the struct on the Elixir side before sending it to \
    the database. This does not work well with the KvStore, where we need to store many types as \
    a blob, but don't care about their actual value on the database side.

    By delegating the conversion to the database, we do not need to manually re-implement all the \
    conversion functions on the elixir side.
  """
  @spec insert(
          %{group: String.t(), key: String.t(), value_type: value_type(), value: term()},
          Keyword.t()
        ) :: :ok
  def insert(kv_store_map, opts \\ []) do
    %{
      group: group,
      key: key,
      value: value,
    } = kv_store_map

    value_type = Map.get(kv_store_map, :value_type)

    value_expr =
      case value_type do
        nil -> value
        :integer -> dynamic([_kv], fragment("intAsBlob(?)", ^value))
        :big_integer -> dynamic([_kv], fragment("bigintAsBlob(?)", ^value))
        :string -> dynamic([_kv], fragment("varcharAsBlob(?)", ^value))
      end

    insert_query =
      from kv in __MODULE__,
        update: [set: [value: ^value_expr]],
        where: kv.group == ^group and kv.key == ^key

    # We can insert a value using an `update` by pinning the primary key using `where` and
    # setting the non-key values with update
    Repo.update_all(insert_query, [], opts)

    :ok
  end

  @spec fetch_value(String.t(), String.t(), value_type()) :: {:ok, term()} | {:error, term()}
  def fetch_value(group, key, value_type \\ nil, opts \\ []) do
    value_expr =
      case value_type do
        nil -> dynamic([kv], kv.value)
        :integer -> dynamic([kv], fragment("blobAsInt(?)", kv.value))
        :big_integer -> dynamic([kv], fragment("blobAsBigint(?)", kv.value))
        :string -> dynamic([kv], fragment("blobAsVarChar(?)", kv.value))
      end

    query = from __MODULE__, select: ^value_expr
    primary_key = [group: group, key: key]

    Repo.fetch_by(query, primary_key, opts)
  end
end
