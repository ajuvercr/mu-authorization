alias LR.Grammar.NonTerminal

defmodule NonTerminal do
  defstruct [:name, :construct]

  def new(name, construct) do
    %NonTerminal{name: name, construct: construct}
  end
end
