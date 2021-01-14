//! We'll imagine a (idealized) card reader which unlocks a door / blinks a light when it's open
//!
//! This is the by-hand version, useful to compare to the macro version in the docs

use state_machine_trait::{StateMachine, TransitionResult};

#[derive(Clone)]
pub enum CardReader {
    Locked(Locked),
    ReadingCard(ReadingCard),
    Unlocked(DoorOpen),
}

#[derive(thiserror::Error, Debug)]
pub enum CardReaderError {}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum CardReaderEvents {
    /// Someone's presented a card for reading
    CardReadable(CardData),
    /// Door latch connected
    DoorClosed,
    CardAccepted,
    CardRejected,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Commands {
    StartBlinkingLight,
    StopBlinkingLight,
    ProcessData(CardData),
}

type CardData = String;

impl CardReader {
    /// Reader starts locked
    pub fn new() -> Self {
        CardReader::Locked(Locked {})
    }
}

impl StateMachine<CardReader, CardReaderEvents, Commands> for CardReader {
    type Error = CardReaderError;

    fn on_event(self, event: CardReaderEvents) -> TransitionResult<Self, Self::Error, Commands> {
        let mut commands = vec![];
        let new_state = match self {
            CardReader::Locked(ls) => match event {
                CardReaderEvents::CardReadable(data) => {
                    commands.push(Commands::ProcessData(data.clone()));
                    commands.push(Commands::StartBlinkingLight);
                    Self::ReadingCard(ls.on_card_readable(data))
                }
                _ => return TransitionResult::InvalidTransition,
            },
            CardReader::ReadingCard(rc) => match event {
                CardReaderEvents::CardAccepted => {
                    commands.push(Commands::StopBlinkingLight);
                    Self::Unlocked(rc.on_card_accepted())
                }
                CardReaderEvents::CardRejected => {
                    commands.push(Commands::StopBlinkingLight);
                    Self::Locked(rc.on_card_rejected())
                }
                _ => return TransitionResult::InvalidTransition,
            },
            CardReader::Unlocked(_) => match event {
                CardReaderEvents::DoorClosed => Self::Locked(Locked {}),
                _ => return TransitionResult::InvalidTransition,
            },
        };
        TransitionResult::Ok {
            commands,
            new_state,
        }
    }

    fn state(&self) -> &CardReader {
        self
    }
}

/// Door is locked / idle / we are ready to read
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Locked {}

/// Actively reading the card
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ReadingCard {
    card_data: CardData,
}

/// The door is open, we shouldn't be accepting cards and should be blinking the light
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DoorOpen {}

impl Locked {
    fn on_card_readable(&self, data: CardData) -> ReadingCard {
        ReadingCard { card_data: data }
    }
}

impl ReadingCard {
    fn on_card_accepted(&self) -> DoorOpen {
        DoorOpen {}
    }
    fn on_card_rejected(&self) -> Locked {
        Locked {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should be kept the same the main example doctest
    #[test]
    fn run_a_card_reader() {
        let cr = CardReader::Locked(Locked {});
        let (cr, cmds) = cr
            .on_event(CardReaderEvents::CardReadable("badguy".to_string()))
            .unwrap();
        assert_eq!(cmds[0], Commands::ProcessData("badguy".to_string()));
        assert_eq!(cmds[1], Commands::StartBlinkingLight);

        let (cr, cmds) = cr.on_event(CardReaderEvents::CardRejected).unwrap();
        assert_eq!(cmds[0], Commands::StopBlinkingLight);

        let (cr, cmds) = cr
            .on_event(CardReaderEvents::CardReadable("goodguy".to_string()))
            .unwrap();
        assert_eq!(cmds[0], Commands::ProcessData("goodguy".to_string()));
        assert_eq!(cmds[1], Commands::StartBlinkingLight);

        let (_, cmds) = cr.on_event(CardReaderEvents::CardAccepted).unwrap();
        assert_eq!(cmds[0], Commands::StopBlinkingLight);
    }
}
