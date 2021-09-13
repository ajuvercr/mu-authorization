alias LR.Grammar.Transition

defmodule Transition do
  defstruct [:item, :from, :to]

  defimpl String.Chars, for: Transition do
    def to_string(%Transition{item: item, from: from, to: to}) do
      inspect(from) <> " " <> Kernel.to_string(item) <> " " <> inspect(to)
    end
  end

  def new(item, from, to) do
    %Transition{item: item, from: from, to: to}
  end

  def with_map(%Transition{from: from, to: to} = self, map) do
    %{self | to: Map.get(map, to), from: Map.get(map, from)}
  end
end
