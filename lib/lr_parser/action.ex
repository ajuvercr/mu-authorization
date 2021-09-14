alias LR.Grammar.Action
alias LR.Grammar.ItemSet

defmodule Action do
  defstruct [:reducers, :transitions]

  def new(ends) do
    %Action{
      reducers: ends,
      transitions: %{}
    }
  end

  def add_transition(self, transition) do
    cond do
      Map.has_key?(self.reducers, transition.item) ->
        IO.inspect(self,
          label: "Cannot add transition when a reducer is present!"
        )

        IO.puts("From: " <> ItemSet.to_string(transition.from))
        IO.inspect(transition.item, label: "Item")
        IO.puts("To: " <> ItemSet.to_string(transition.to))

        self

      Map.has_key?(self.transitions, transition.item) ->
        IO.inspect(self,
          label: "Overriding transition!"
        )

        IO.puts("From: " <> ItemSet.to_string(transition.from))
        IO.inspect(transition.item, label: "Item")
        IO.puts("To: " <> ItemSet.to_string(transition.to))

        self

      true ->
        new_transitions = self.transitions |> Map.put(transition.item, transition.to)
        %{self | transitions: new_transitions}
    end
  end

  def add_transition_in(transition, transitions) do
    # put_in(transitions[][:transitions][transition.item], transition.to)

    update_in(transitions[transition.from], &add_transition(&1, transition))
  end
end
