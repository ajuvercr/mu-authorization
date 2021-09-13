alias LR.Grammar.Action

defmodule Action do
  defstruct [:reducers, :transitions]

  def new(ends) do
    %Action{
      reducers: ends,
      transitions: %{}
    }
  end

  def add_transition(self, transition) do
    if Map.has_key?(self.reducers, transition.item) do
      IO.inspect({self, transition.item},
        label: "Cannot add transition when a reducer is present!"
      )
    end

    new_transitions = self.transitions |> Map.put(transition.item, transition.to)

    %{self | transitions: new_transitions}
  end

  def add_transition_in(transition, transitions) do
    # put_in(transitions[][:transitions][transition.item], transition.to)

    update_in(transitions[transition.from], &add_transition(&1, transition))
  end
end
