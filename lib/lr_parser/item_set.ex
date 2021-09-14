alias LR.Grammar.Item
alias LR.Grammar.ItemSet

defmodule ItemSet do
  # defstruct [:items]

  def to_string(items) do
    interior =
      items
      |> Enum.map(&Kernel.to_string/1)
      |> Enum.join("    ")

    "ItemSet[#{interior}]"
  end

  def new do
    MapSet.new()
  end

  def new(items) do
    Enum.reduce(
      items,
      MapSet.new(),
      &ItemSet.add_item(&2, &1)
    )
  end

  def size(items) do
    MapSet.size(items)
  end

  def add_item(items, %Item{} = item) do
    MapSet.put(items, item)
  end

  def add_item(items, item) do
    MapSet.put(items, item |> Item.new())
  end

  def add_items(self, items) do
    Enum.reduce(items, self, &ItemSet.add_item(&2, &1))
  end
end
