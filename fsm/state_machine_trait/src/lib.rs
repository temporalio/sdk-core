use std::error::Error;

/// This trait defines a state machine (more formally, a [finite state
/// transducer](https://en.wikipedia.org/wiki/Finite-state_transducer)) which accepts events (the
/// input alphabet), uses them to mutate itself, and (may) output some commands (the output
/// alphabet) as a result.
///
/// The `State`, `Event`, and `Command` type parameters will generally be enumerations.
// TODO: Needed for `get_shared_state`?
// `SharedState` is expected to be a struct.
pub trait StateMachine<State, Event, Command> {
    /// The error type produced by this state machine when handling events
    type Error: Error;

    /// Handle an incoming event
    // TODO: Taking by ref here means we copy unnecessarily, but, who cares? The data is small.
    //  The advantage is we get a "transactional" history / capability. Consuming self would be
    //  technically more efficient.
    fn on_event(self, event: Event) -> TransitionResult<State, Self::Error, Command>;

    /// Returns the current state of the machine
    fn state(&self) -> &State;
}

// TODO: Likely need to return existing state with invalid trans/err as long as on_event consumes
/// A transition result is emitted every time the [StateMachine] handles an event.
pub enum TransitionResult<MachineState, StateMachineError, StateMachineCommand> {
    /// This state does not define a transition for this event from this state. All other errors
    /// should use the [Err](enum.TransitionResult.html#variant.Err) variant.
    InvalidTransition,
    /// The transition was successful
    Ok {
        commands: Vec<StateMachineCommand>,
        new_state: MachineState,
    },
    /// There was some error performing the transition
    Err(StateMachineError),
}

impl<S, E, C> TransitionResult<S, E, C> {
    /// Produce a transition with the provided commands to the provided state
    pub fn ok<CI, IS>(commands: CI, new_state: IS) -> Self
    where
        CI: IntoIterator<Item = C>,
        IS: Into<S>,
    {
        Self::Ok {
            commands: commands.into_iter().collect(),
            new_state: new_state.into(),
        }
    }

    /// Produce a transition with no commands relying on [Default] for the destination state's
    /// value
    pub fn default<DestState>() -> Self
    where
        DestState: Into<S> + Default,
    {
        Self::Ok {
            commands: vec![],
            new_state: DestState::default().into(),
        }
    }

    /// Uses `Into` to produce a transition with no commands from the provided current state to
    /// the provided (by type parameter) destination state.
    pub fn from<CurrentState, DestState>(current_state: CurrentState) -> Self
    where
        DestState: Into<S>,
        CurrentState: Into<DestState>,
    {
        let as_dest: DestState = current_state.into();
        Self::Ok {
            commands: vec![],
            new_state: as_dest.into(),
        }
    }

    // TODO: Make test only or something?
    pub fn unwrap(self) -> (S, Vec<C>) {
        match self {
            Self::Ok {
                commands,
                new_state,
            } => (new_state, commands),
            _ => panic!("Transition was not successful!"),
        }
    }
}
