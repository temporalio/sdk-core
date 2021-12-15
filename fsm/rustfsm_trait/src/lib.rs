use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

/// This trait defines a state machine (more formally, a [finite state
/// transducer](https://en.wikipedia.org/wiki/Finite-state_transducer)) which accepts events (the
/// input alphabet), uses them to mutate itself, and (may) output some commands (the output
/// alphabet) as a result.
pub trait StateMachine: Sized {
    /// The error type produced by this state machine when handling events
    type Error: Error;
    /// The type used to represent different machine states. Should be an enum.
    type State;
    /// The type used to represent state that common among all states. Should be a struct.
    type SharedState;
    /// The type used to represent events the machine handles. Should be an enum.
    type Event;
    /// The type used to represent commands the machine issues upon transitions.
    type Command;

    /// Handle an incoming event, returning a transition result which represents updates to apply
    /// to the state machine.
    fn on_event(self, event: Self::Event) -> TransitionResult<Self, Self::State>;

    /// Handle an incoming event and mutate the state machine to update to the new state and apply
    /// any changes to shared state.
    ///
    /// Returns the commands issued by the transition on success, otherwise a [MachineError]
    fn on_event_mut(
        &mut self,
        event: Self::Event,
    ) -> Result<Vec<Self::Command>, MachineError<Self::Error>>
    where
        Self: Clone,
    {
        // NOTE: This clone is actually nice in some sense, giving us a kind of transactionality.
        //   However if there are really big things in state it could be an issue.
        let res = self.clone().on_event(event);
        match res {
            TransitionResult::Ok {
                commands,
                new_state,
                shared_state,
            } => {
                *self = Self::from_parts(shared_state, new_state);
                Ok(commands)
            }
            TransitionResult::OkNoShare {
                commands,
                new_state,
            } => {
                self.set_state(new_state);
                Ok(commands)
            }
            TransitionResult::InvalidTransition => Err(MachineError::InvalidTransition),
            TransitionResult::Err(e) => Err(MachineError::Underlying(e)),
        }
    }

    fn name(&self) -> &str;

    /// Returns the current state of the machine
    fn state(&self) -> &Self::State;
    fn set_state(&mut self, new_state: Self::State);

    /// Returns the current shared state of the machine
    fn shared_state(&self) -> &Self::SharedState;

    /// Returns true if the machine's current state is a final one
    fn has_reached_final_state(&self) -> bool;

    /// Given the shared data and new state, create a new instance.
    fn from_parts(shared: Self::SharedState, state: Self::State) -> Self;

    /// Return a PlantUML definition of the fsm that can be used to visualize it
    fn visualizer() -> &'static str;
}

/// The error returned by [StateMachine]s when handling events
#[derive(Debug)]
pub enum MachineError<E: Error> {
    /// An undefined transition was attempted
    InvalidTransition,
    /// Some error occurred while processing the transition
    Underlying(E),
}

impl<E: Error> From<E> for MachineError<E> {
    fn from(e: E) -> Self {
        Self::Underlying(e)
    }
}

impl<E: Error> Display for MachineError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineError::InvalidTransition => f.write_str("Invalid transition"),
            MachineError::Underlying(e) => Display::fmt(&e, f),
        }
    }
}
impl<E: Error> Error for MachineError<E> {}

pub enum MachineUpdate<Machine>
where
    Machine: StateMachine,
{
    InvalidTransition,
    Ok { commands: Vec<Machine::Command> },
}

impl<M> MachineUpdate<M>
where
    M: StateMachine,
{
    /// Unwraps the machine update, panicking if the transition was invalid.
    pub fn unwrap(self) -> Vec<M::Command> {
        match self {
            Self::Ok { commands } => commands,
            Self::InvalidTransition => panic!("Transition was not successful!"),
        }
    }
}

/// A transition result is emitted every time the [StateMachine] handles an event.
pub enum TransitionResult<Machine, DestinationState>
where
    Machine: StateMachine,
    DestinationState: Into<Machine::State>,
{
    /// This state does not define a transition for this event from this state. All other errors
    /// should use the [Err](enum.TransitionResult.html#variant.Err) variant.
    InvalidTransition,
    /// The transition was successful
    Ok {
        commands: Vec<Machine::Command>,
        new_state: DestinationState,
        shared_state: Machine::SharedState,
    },
    /// The transition was successful with no shared state change
    OkNoShare {
        commands: Vec<Machine::Command>,
        new_state: DestinationState,
    },
    /// There was some error performing the transition
    Err(Machine::Error),
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State>,
{
    /// Produce a transition with the provided commands to the provided state. No changes to shared
    /// state if it exists
    pub fn ok<CI>(commands: CI, new_state: Ds) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
    {
        Self::OkNoShare {
            commands: commands.into_iter().collect(),
            new_state,
        }
    }

    /// Produce a transition with the provided commands to the provided state with shared state
    /// changes
    pub fn ok_shared<CI, SS>(commands: CI, new_state: Ds, new_shared: SS) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
        SS: Into<Sm::SharedState>,
    {
        Self::Ok {
            commands: commands.into_iter().collect(),
            new_state,
            shared_state: new_shared.into(),
        }
    }

    /// Uses `Into` to produce a transition with no commands from the provided current state to
    /// the provided (by type parameter) destination state.
    pub fn from<CurrentState>(current_state: CurrentState) -> Self
    where
        CurrentState: Into<Ds>,
    {
        let as_dest: Ds = current_state.into();
        Self::OkNoShare {
            commands: vec![],
            new_state: as_dest,
        }
    }
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State> + Default,
{
    /// Produce a transition with commands relying on [Default] for the destination state's value
    pub fn commands<CI>(commands: CI) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
    {
        Self::OkNoShare {
            commands: commands.into_iter().collect(),
            new_state: Ds::default(),
        }
    }

    /// Produce a transition with no commands relying on [Default] for the destination state's
    /// value
    pub fn default() -> Self {
        Self::OkNoShare {
            commands: vec![],
            new_state: Ds::default(),
        }
    }
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State>,
{
    /// Turns more-specific (struct) transition result into more-general (enum) transition result
    pub fn into_general(self) -> TransitionResult<Sm, Sm::State> {
        match self {
            TransitionResult::InvalidTransition => TransitionResult::InvalidTransition,
            TransitionResult::Ok {
                commands,
                new_state,
                shared_state,
            } => TransitionResult::Ok {
                commands,
                new_state: new_state.into(),
                shared_state,
            },
            TransitionResult::OkNoShare {
                commands,
                new_state,
            } => TransitionResult::OkNoShare {
                commands,
                new_state: new_state.into(),
            },
            TransitionResult::Err(e) => TransitionResult::Err(e),
        }
    }
}
