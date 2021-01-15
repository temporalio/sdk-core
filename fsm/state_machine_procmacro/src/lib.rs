extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use std::collections::{HashMap, HashSet};
use syn::{
    parenthesized,
    parse::{Parse, ParseStream, Result},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
    Error, Fields, Ident, Token, Type, Variant,
};

/// Parses a DSL for defining finite state machines, and produces code implementing the
/// [StateMachine](trait.StateMachine.html) trait.
///
/// An example state machine definition of a card reader for unlocking a door:
/// ```
/// # extern crate state_machine_trait as rustfsm;
/// use state_machine_procmacro::fsm;
/// use std::convert::Infallible;
/// use state_machine_trait::{StateMachine, TransitionResult};
///
/// fsm! {
///     name CardReader; command Commands; error Infallible; shared_state SharedState;
///
///     Locked --(CardReadable(CardData), shared on_card_readable) --> ReadingCard;
///     Locked --(CardReadable(CardData), shared on_card_readable) --> Locked;
///     ReadingCard --(CardAccepted, on_card_accepted) --> DoorOpen;
///     ReadingCard --(CardRejected, on_card_rejected) --> Locked;
///     DoorOpen --(DoorClosed, on_door_closed) --> Locked;
/// }
///
/// #[derive(Clone)]
/// pub struct SharedState {
///     last_id: Option<String>
/// }
///
/// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// pub enum Commands {
///     StartBlinkingLight,
///     StopBlinkingLight,
///     ProcessData(CardData),
/// }
///
/// type CardData = String;
///
/// /// Door is locked / idle / we are ready to read
/// #[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
/// pub struct Locked {}
///
/// /// Actively reading the card
/// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// pub struct ReadingCard {
///     card_data: CardData,
/// }
///
/// /// The door is open, we shouldn't be accepting cards and should be blinking the light
/// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// pub struct DoorOpen {}
/// impl DoorOpen {
///     fn on_door_closed(&self) -> CardReaderTransition {
///         TransitionResult::ok(vec![], Locked {})
///     }
/// }
///
/// impl Locked {
///     fn on_card_readable(&self, shared_dat: SharedState, data: CardData) -> CardReaderTransition {
///         match shared_dat.last_id {
///             // Arbitrarily deny the same person entering twice in a row
///             Some(d) if d == data => TransitionResult::default::<Locked>(),
///             _ => {
///                 // Otherwise issue a processing command. This illustrates using the same handler
///                 // for different destinations
///                 TransitionResult::ok_shared(
///                     vec![
///                         Commands::ProcessData(data.clone()),
///                         Commands::StartBlinkingLight,
///                     ],
///                     ReadingCard { card_data: data.clone() },
///                     SharedState { last_id: Some(data) }
///                 )
///             }   
///         }
///     }
/// }
///
/// impl ReadingCard {
///     fn on_card_accepted(&self) -> CardReaderTransition {
///         TransitionResult::ok(vec![Commands::StopBlinkingLight], DoorOpen {})
///     }
///     fn on_card_rejected(&self) -> CardReaderTransition {
///         TransitionResult::ok(vec![Commands::StopBlinkingLight], Locked {})
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let crs = CardReaderState::Locked(Locked {});
/// let mut cr = CardReader { state: crs, shared_state: SharedState { last_id: None } };
/// let cmds = cr.on_event_mut(CardReaderEvents::CardReadable("badguy".to_string()))?;
/// assert_eq!(cmds[0], Commands::ProcessData("badguy".to_string()));
/// assert_eq!(cmds[1], Commands::StartBlinkingLight);
///
/// let cmds = cr.on_event_mut(CardReaderEvents::CardRejected)?;
/// assert_eq!(cmds[0], Commands::StopBlinkingLight);
///
/// let cmds = cr.on_event_mut(CardReaderEvents::CardReadable("goodguy".to_string()))?;
/// assert_eq!(cmds[0], Commands::ProcessData("goodguy".to_string()));
/// assert_eq!(cmds[1], Commands::StartBlinkingLight);
///
/// let cmds = cr.on_event_mut(CardReaderEvents::CardAccepted)?;
/// assert_eq!(cmds[0], Commands::StopBlinkingLight);
/// # Ok(())
/// # }
/// ```
///None
/// In the above example the first word is the name of the state machine, then after the comma the
/// type (which you must define separately) of commands produced by the machine.
///
/// then each line represents a transition, where the first word is the initial state, the tuple
/// inside the arrow is `(eventtype[, event handler])`, and the word after the arrow is the
/// destination state. here `eventtype` is an enum variant , and `event_handler` is a function you
/// must define outside the enum whose form depends on the event variant. the only variant types
/// allowed are unit and one-item tuple variants. For unit variants, the function takes no
/// parameters. For the tuple variants, the function takes the variant data as its parameter. In
/// either case the function is expected to return a `TransitionResult` to the appropriate state.
///
/// The first transition can be interpreted as "If the machine is in the locked state, when a
/// `CardReadable` event is seen, call `on_card_readable` (pasing in `CardData`) and transition to
/// the `ReadingCard` state.
///
/// The macro will generate a few things:
/// * A struct for the overall state machine, named with the provided name. Here:
///   ```ignore
///   struct CardMachine {
///       state: CardMachineState,
///       shared_state: CardId,
///   }
///   ```
/// * An enum with a variant for each state, named with the provided name + "State".
///   ```ignore
///   enum CardMachineState {
///       Locked(Locked),
///       ReadingCard(ReadingCard),
///       Unlocked(Unlocked),
///   }
///   ```
///
///   You are expected to define a type for each state, to contain that state's data. If there is
///   no data, you can simply: `type StateName = ()`
/// * An enum with a variant for each event. You are expected to define the type (if any) contained
///   in the event variant.
///   ```ignore
///   enum CardMachineEvents {
///     CardReadable(CardData)
///   }
///   ```
/// * An implementation of the [StateMachine](trait.StateMachine.html) trait for the generated state
///   machine enum (in this case, `CardMachine`)
/// * A type alias for a [TransitionResult](enum.TransitionResult.html) with the appropriate generic
///   parameters set for your machine. It is named as your machine with `Transition` appended. In
///   this case, `CardMachineTransition`.
#[proc_macro]
pub fn fsm(input: TokenStream) -> TokenStream {
    let def: StateMachineDefinition = parse_macro_input!(input as StateMachineDefinition);
    def.codegen()
}

mod kw {
    syn::custom_keyword!(name);
    syn::custom_keyword!(command);
    syn::custom_keyword!(error);
    syn::custom_keyword!(shared);
    syn::custom_keyword!(shared_state);
}

struct StateMachineDefinition {
    name: Ident,
    shared_state_type: Option<Type>,
    command_type: Ident,
    error_type: Ident,
    transitions: HashSet<Transition>,
}

impl Parse for StateMachineDefinition {
    // TODO: Pub keyword
    fn parse(input: ParseStream) -> Result<Self> {
        // First parse the state machine name, command type, and error type
        let (name, command_type, error_type, shared_state_type) = parse_machine_types(&input).map_err(|mut e| {
            e.combine(Error::new(
                e.span(),
                "The fsm definition should begin with `name MachineName; command CommandType; error ErrorType;` optionally followed by `shared_state SharedStateType;`",
            ));
            e
        })?;
        // Then the state machine definition is simply a sequence of transitions separated by
        // semicolons
        let transitions: Punctuated<Transition, Token![;]> =
            input.parse_terminated(Transition::parse)?;
        let transitions = transitions.into_iter().collect();
        Ok(Self {
            name,
            shared_state_type,
            transitions,
            command_type,
            error_type,
        })
    }
}

fn parse_machine_types(input: &ParseStream) -> Result<(Ident, Ident, Ident, Option<Type>)> {
    let _: kw::name = input.parse()?;
    let name: Ident = input.parse()?;
    input.parse::<Token![;]>()?;

    let _: kw::command = input.parse()?;
    let command_type: Ident = input.parse()?;
    input.parse::<Token![;]>()?;

    let _: kw::error = input.parse()?;
    let error_type: Ident = input.parse()?;
    input.parse::<Token![;]>()?;

    let shared_state_type: Option<Type> = if input.peek(kw::shared_state) {
        let _: kw::shared_state = input.parse()?;
        let typep = input.parse()?;
        input.parse::<Token![;]>()?;
        Some(typep)
    } else {
        None
    };
    Ok((name, command_type, error_type, shared_state_type))
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Transition {
    from: Ident,
    to: Ident,
    event: Variant,
    handler: Option<Ident>,
    mutates_shared: bool,
}

impl Parse for Transition {
    fn parse(input: ParseStream) -> Result<Self> {
        // TODO: Use keywords instead of implicit placement, or other better arg-passing method
        //  maybe `enum MachineName<Command, Error>`
        //  and need start state
        // TODO: Currently the handlers are not required to transition to the state they claimed
        //   they would. It would be great to find a way to fix that.
        // TODO: It is desirable to be able to define immutable state the machine is initialized
        //   with and is accessible from all states.
        // Parse the initial state name
        let from: Ident = input.parse()?;
        // Parse at least one dash
        input.parse::<Token![-]>()?;
        while input.peek(Token![-]) {
            input.parse::<Token![-]>()?;
        }
        // Parse transition information inside parens
        let transition_info;
        parenthesized!(transition_info in input);
        // Get the event variant definition
        let event: Variant = transition_info.parse()?;
        // Reject non-unit or single-item-tuple variants
        match &event.fields {
            Fields::Named(_) => {
                return Err(Error::new(
                    event.span(),
                    "Struct variants are not supported for events",
                ))
            }
            Fields::Unnamed(uf) => {
                if uf.unnamed.len() != 1 {
                    return Err(Error::new(
                        event.span(),
                        "Only tuple variants with exactly one item are supported for events",
                    ));
                }
            }
            Fields::Unit => {}
        }
        // Check if there is an event handler, and parse it
        let (mutates_shared, handler) = if transition_info.peek(Token![,]) {
            transition_info.parse::<Token![,]>()?;
            // Check for mut keyword signifying handler wants to mutate shared state
            let mutates = if transition_info.peek(kw::shared) {
                transition_info.parse::<kw::shared>()?;
                true
            } else {
                false
            };
            (mutates, Some(transition_info.parse()?))
        } else {
            (false, None)
        };
        // Parse at least one dash followed by the "arrow"
        input.parse::<Token![-]>()?;
        while input.peek(Token![-]) {
            input.parse::<Token![-]>()?;
        }
        input.parse::<Token![>]>()?;
        // Parse the destination state
        let to: Ident = input.parse()?;

        Ok(Self {
            from,
            event,
            handler,
            to,
            mutates_shared,
        })
    }
}

impl StateMachineDefinition {
    fn codegen(&self) -> TokenStream {
        // First extract all of the states into a set, and build the enum's insides
        let states: HashSet<_> = self
            .transitions
            .iter()
            .flat_map(|t| vec![t.from.clone(), t.to.clone()])
            .collect();
        let state_variants = states.iter().map(|s| {
            quote! {
                #s(#s)
            }
        });
        let name = &self.name;
        let state_enum_name = Ident::new(&format!("{}State", name), name.span());
        // If user has not defined any shared state, use the unit type.
        let shared_state_type = self
            .shared_state_type
            .clone()
            .unwrap_or_else(|| syn::parse_str("()").unwrap());
        let machine_struct = quote! {
            #[derive(Clone)]
            pub struct #name {
                state: #state_enum_name,
                shared_state: #shared_state_type
            }
        };
        let states_enum = quote! {
            #[derive(::derive_more::From, Clone)]
            pub enum #state_enum_name {
                #(#state_variants),*
            }
        };

        // Build the events enum
        let events: HashSet<Variant> = self.transitions.iter().map(|t| t.event.clone()).collect();
        let events_enum_name = Ident::new(&format!("{}Events", name), name.span());
        let events: Vec<_> = events.into_iter().collect();
        let events_enum = quote! {
            pub enum #events_enum_name {
                #(#events),*
            }
        };

        // Construct the trait implementation
        let cmd_type = &self.command_type;
        let err_type = &self.error_type;
        let mut statemap: HashMap<Ident, Vec<Transition>> = HashMap::new();
        for t in &self.transitions {
            statemap
                .entry(t.from.clone())
                .and_modify(|v| v.push(t.clone()))
                .or_insert_with(|| vec![t.clone()]);
        }
        // Add any states without any transitions to the map
        for s in &states {
            if !statemap.contains_key(s) {
                statemap.insert(s.clone(), vec![]);
            }
        }
        let state_branches = statemap.iter().map(|(from, transitions)| {
            let event_branches = transitions
                .iter()
                .map(|ts| {
                    let ev_variant = &ts.event.ident;
                    if let Some(ts_fn) = ts.handler.clone() {
                        let span = ts_fn.span();
                        match ts.event.fields {
                            Fields::Unnamed(_) => {
                                let arglist = if ts.mutates_shared {
                                    quote! {self.shared_state, val}
                                } else {
                                    quote! {val}
                                };
                                quote_spanned! {span=>
                                    #events_enum_name::#ev_variant(val) => {
                                        state_data.#ts_fn(#arglist)
                                    }
                                }
                            }
                            Fields::Unit => {
                                let arglist = if ts.mutates_shared {
                                    quote! {self.shared_state}
                                } else {
                                    quote! {}
                                };
                                quote_spanned! {span=>
                                    #events_enum_name::#ev_variant => {
                                        state_data.#ts_fn(#arglist)
                                    }
                                }
                            }
                            Fields::Named(_) => unreachable!(),
                        }
                    } else {
                        // If events do not have a handler, attempt to construct the next state
                        // using `Default`.
                        let new_state = ts.to.clone();
                        let span = new_state.span();
                        let default_trans = quote_spanned! {span=>
                            TransitionResult::from::<#from, #new_state>(state_data)
                        };
                        let span = ts.event.span();
                        match ts.event.fields {
                            Fields::Unnamed(_) => quote_spanned! {span=>
                                #events_enum_name::#ev_variant(_val) => {
                                    #default_trans
                                }
                            },
                            Fields::Unit => quote_spanned! {span=>
                                #events_enum_name::#ev_variant => {
                                    #default_trans
                                }
                            },
                            Fields::Named(_) => unreachable!(),
                        }
                    }
                })
                // Since most states won't handle every possible event, return an error to that effect
                .chain(std::iter::once(
                    quote! { _ => { return TransitionResult::InvalidTransition } },
                ));
            quote! {
                #state_enum_name::#from(state_data) => match event {
                    #(#event_branches),*
                }
            }
        });

        let trait_impl = quote! {
            impl ::rustfsm::StateMachine for #name {
                type Error = #err_type;
                type State = #state_enum_name;
                type SharedState = #shared_state_type;
                type Event = #events_enum_name;
                type Command = #cmd_type;

                fn on_event(self, event: #events_enum_name)
                  -> ::rustfsm::TransitionResult<Self> {
                    match self.state {
                        #(#state_branches),*
                    }
                }

                fn state(&self) -> &Self::State {
                    &self.state
                }
                fn set_state(&mut self, new: Self::State) {
                    self.state = new
                }

                fn shared_state(&self) -> &Self::SharedState{
                    &self.shared_state
                }

                fn from_parts(shared: Self::SharedState, state: Self::State) -> Self {
                    Self { shared_state: shared, state }
                }
            }
        };

        let transition_result_name = Ident::new(&format!("{}Transition", name), name.span());
        let transition_type_alias = quote! {
            type #transition_result_name = TransitionResult<#name>;
        };

        let output = quote! {
            #transition_type_alias
            #machine_struct
            #states_enum
            #events_enum
            #trait_impl
        };

        output.into()
    }
}
