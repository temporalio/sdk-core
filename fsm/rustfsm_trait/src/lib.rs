use std::{
    error::Error,
    fmt::{Display, Formatter},
};

pub trait StateMachine: Sized {
    type Error: Error;
    type State;
    type SharedState;
    type Event;
    type Command;

    fn on_event(&mut self, event: Self::Event)
        -> Result<Vec<Self::Command>, MachineError<Self::Error>>;

    fn name(&self) -> &str;
    fn state(&self) -> &Self::State;
    fn set_state(&mut self, new_state: Self::State);
    fn shared_state(&self) -> &Self::SharedState;
    fn has_reached_final_state(&self) -> bool;
    fn from_parts(state: Self::State, shared: Self::SharedState) -> Self;
    fn visualizer() -> &'static str;
}

pub enum MachineError<E: Error> {
    InvalidTransition,
    Underlying(E),
}

impl<E: Error> From<E> for MachineError<E> {
    fn from(e: E) -> Self { Self::Underlying(e) }
}

impl<E: Error> std::fmt::Debug for MachineError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineError::InvalidTransition => f.write_str("Invalid transition"),
            MachineError::Underlying(e) => std::fmt::Debug::fmt(e, f),
        }
    }
}

impl<E: Error> std::fmt::Display for MachineError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineError::InvalidTransition => f.write_str("Invalid transition"),
            MachineError::Underlying(e) => Display::fmt(e, f),
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
    pub fn unwrap(self) -> Vec<M::Command> {
        match self {
            Self::Ok { commands } => commands,
            Self::InvalidTransition => panic!("Transition was not successful!"),
        }
    }
}

pub enum TransitionResult<Machine, DestinationState>
where
    Machine: StateMachine,
    DestinationState: Into<Machine::State>,
{
    InvalidTransition,
    Ok { commands: Vec<Machine::Command>, new_state: DestinationState },
    Err(Machine::Error),
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State>,
{
    pub fn ok<CI>(commands: CI, new_state: Ds) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
    {
        Self::Ok { commands: commands.into_iter().collect(), new_state }
    }

    pub fn from<CurrentState>(current_state: CurrentState) -> Self
    where
        CurrentState: Into<Ds>,
    {
        let as_dest: Ds = current_state.into();
        Self::Ok { commands: vec![], new_state: as_dest }
    }
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State> + Default,
{
    pub fn commands<CI>(commands: CI) -> Self
    where
        CI: IntoIterator<Item = Sm::Command>,
    {
        Self::Ok { commands: commands.into_iter().collect(), new_state: Ds::default() }
    }
}

impl<Sm, Ds> Default for TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State> + Default,
{
    fn default() -> Self {
        Self::Ok { commands: vec![], new_state: Ds::default() }
    }
}

impl<Sm, Ds> TransitionResult<Sm, Ds>
where
    Sm: StateMachine,
    Ds: Into<Sm::State>,
{
    pub fn into_general(self) -> TransitionResult<Sm, Sm::State> {
        match self {
            TransitionResult::InvalidTransition => TransitionResult::InvalidTransition,
            TransitionResult::Ok { commands, new_state } => TransitionResult::Ok {
                commands,
                new_state: new_state.into(),
            },
            TransitionResult::Err(e) => TransitionResult::Err(e),
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn into_cmd_result(self) -> Result<(Vec<Sm::Command>, Sm::State), MachineError<Sm::Error>> {
        let general = self.into_general();
        match general {
            TransitionResult::Ok { new_state, commands } => Ok((commands, new_state)),
            TransitionResult::InvalidTransition => Err(MachineError::InvalidTransition),
            TransitionResult::Err(e) => Err(MachineError::Underlying(e)),
        }
    }
}