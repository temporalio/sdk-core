#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, WorkflowContext, WorkflowResult, WorkflowTermination,
    activities::{ActivityContext, ActivityError},
};

#[derive(Debug)]
enum Booking {
    Hotel,
    Flight,
    Car,
}

#[workflow]
#[derive(Default)]
pub struct SagaWorkflow;

#[workflow_methods]
impl SagaWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        trip_id: String,
    ) -> WorkflowResult<Vec<String>> {
        let mut compensations: Vec<(Booking, String)> = Vec::new();

        match Self::book_trip(ctx, trip_id, &mut compensations).await {
            Ok(()) => Ok(compensations.into_iter().map(|(_, id)| id).collect()),
            Err(e) => {
                Self::run_compensations(ctx, &compensations).await;
                Err(WorkflowTermination::failed(e))
            }
        }
    }

    async fn book_trip(
        ctx: &mut WorkflowContext<SagaWorkflow>,
        trip_id: String,
        compensations: &mut Vec<(Booking, String)>,
    ) -> Result<(), ActivityExecutionError> {
        let hotel = ctx
            .start_activity(
                BookingActivities::book_hotel,
                trip_id.clone(),
                activity_opts(),
            )
            .await?;
        compensations.push((Booking::Hotel, hotel));

        let flight = ctx
            .start_activity(
                BookingActivities::book_flight,
                trip_id.clone(),
                activity_opts(),
            )
            .await?;
        compensations.push((Booking::Flight, flight));

        let car = ctx
            .start_activity(
                BookingActivities::book_car,
                trip_id.clone(),
                activity_opts(),
            )
            .await?;
        compensations.push((Booking::Car, car));

        Ok(())
    }

    async fn run_compensations<W>(
        ctx: &mut WorkflowContext<W>,
        compensations: &[(Booking, String)],
    ) {
        for (service, booking_id) in compensations.iter().rev() {
            let result = match *service {
                Booking::Hotel => {
                    ctx.start_activity(
                        BookingActivities::cancel_hotel,
                        booking_id.clone(),
                        activity_opts(),
                    )
                    .await
                }
                Booking::Flight => {
                    ctx.start_activity(
                        BookingActivities::cancel_flight,
                        booking_id.clone(),
                        activity_opts(),
                    )
                    .await
                }
                Booking::Car => {
                    ctx.start_activity(
                        BookingActivities::cancel_car,
                        booking_id.clone(),
                        activity_opts(),
                    )
                    .await
                }
            };

            if let Err(e) = result {
                eprintln!("Compensation failed for {service:?} {booking_id}: {e}");
            }
        }
    }
}

pub struct BookingActivities;

#[activities]
impl BookingActivities {
    #[activity]
    pub async fn book_hotel(
        _ctx: ActivityContext,
        trip_id: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("hotel-{trip_id}"))
    }

    #[activity]
    pub async fn book_flight(
        _ctx: ActivityContext,
        trip_id: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("flight-{trip_id}"))
    }

    #[activity]
    pub async fn book_car(_ctx: ActivityContext, trip_id: String) -> Result<String, ActivityError> {
        if trip_id.contains("fail") {
            return Err(ActivityError::NonRetryable(
                anyhow::anyhow!("Car booking failed for trip {trip_id}").into(),
            ));
        }
        Ok(format!("car-{trip_id}"))
    }

    #[activity]
    pub async fn cancel_hotel(
        _ctx: ActivityContext,
        booking_id: String,
    ) -> Result<(), ActivityError> {
        println!("Cancelled hotel booking: {booking_id}");
        Ok(())
    }

    #[activity]
    pub async fn cancel_flight(
        _ctx: ActivityContext,
        booking_id: String,
    ) -> Result<(), ActivityError> {
        println!("Cancelled flight booking: {booking_id}");
        Ok(())
    }

    #[activity]
    pub async fn cancel_car(
        _ctx: ActivityContext,
        booking_id: String,
    ) -> Result<(), ActivityError> {
        println!("Cancelled car booking: {booking_id}");
        Ok(())
    }
}

fn activity_opts() -> ActivityOptions {
    ActivityOptions::start_to_close_timeout(Duration::from_secs(1))
}
