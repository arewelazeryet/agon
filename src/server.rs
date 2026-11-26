use std::{env, sync::Arc};

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use color_eyre::eyre::{self, Context, Result, eyre};
use futures::{StreamExt, TryStreamExt, stream};
use mnemosyne::database::{
    Database,
    performance_rankings::{PerformanceRankingRow, PerformanceRuleset},
};
use rosu_v2::{Osu, model::GameMode};
use serde::{Deserialize, Serialize};

use crate::server::Variant::{Four, Seven};

/// Number of ranking pages fetched per request.
const PAGES: u32 = 200;

const MAX_CONCURRENT: usize = 10;

pub type SharedServer = Arc<Server>;

pub struct Server {
    database: Database,
    osu_client: Osu,
}

#[derive(Deserialize)]
pub struct RankingQuery {
    variant: String,
}

#[derive(Serialize)]
pub struct RankingResponse {
    variant: String,
    stored: usize,
}

/// Maps a query variant to its osu! mode and mania sub-variant.
fn resolve_variant(variant: &str) -> Result<PerformanceRuleset> {
    match variant {
        "osu" => Ok(PerformanceRuleset::Osu),
        "taiko" => Ok(PerformanceRuleset::Taiko),
        "catch" => Ok(PerformanceRuleset::Catch),
        "maniaall" => Ok(PerformanceRuleset::ManiaAll),
        "mania4k" => Ok(PerformanceRuleset::Mania4k),
        "mania7k" => Ok(PerformanceRuleset::Mania7k),
        other => Err(eyre!("Unknown ranking variant: {other}")),
    }
}

#[derive(Debug, Clone, Copy)]
enum Variant {
    Four,
    Seven,
}

fn form_request(mode: PerformanceRuleset) -> (GameMode, Option<Variant>) {
    match mode {
        PerformanceRuleset::Osu => (GameMode::Osu, None),
        PerformanceRuleset::Taiko => (GameMode::Taiko, None),
        PerformanceRuleset::Catch => (GameMode::Catch, None),
        PerformanceRuleset::ManiaAll => (GameMode::Mania, None),
        PerformanceRuleset::Mania4k => (GameMode::Mania, Some(Four)),
        PerformanceRuleset::Mania7k => (GameMode::Mania, Some(Seven)),
    }
}

impl Server {
    pub async fn init() -> Result<Self> {
        tracing::info!("Initializing server dependencies");

        let database_url = env::var("DATABASE_URL")?;
        let client_id: u64 = env::var("OSU_API_CLIENT_ID")?.parse()?;
        let client_secret = env::var("OSU_API_CLIENT_SECRET")?;

        tracing::debug!("Connecting to database");
        let database = Database::new(&database_url).await?;

        tracing::debug!(client_id, "Building osu! API client");
        let osu_client = rosu_v2::OsuBuilder::new()
            .client_id(client_id)
            .client_secret(client_secret)
            .ratelimit(15)
            .build()
            .await?;

        tracing::info!("Server initialization complete");

        Ok(Self {
            database,
            osu_client,
        })
    }

    /// Fetches all ranking pages and stores them, returning the stored row count.
    pub async fn fetch_and_store(&self, variant: &str) -> Result<usize> {
        let mode = resolve_variant(variant)?;

        // One snapshot timestamp for the whole fetch: the hypertable is keyed on it.
        let fetched_at = chrono::Utc::now();
        let rows = stream::iter(1..=PAGES)
            .map(|page| async move {
                let (rosu_mode, variant) = form_request(mode);

                let request = self.osu_client.performance_rankings(rosu_mode).page(page);

                let request = match variant {
                    Some(Four) => request.variant_4k(),
                    Some(Seven) => request.variant_7k(),
                    None => request,
                };

                let response = request
                    .await
                    .wrap_err("Failed to fetch performance ranking page")?;

                let mut rows = Vec::new();

                for entry in response.ranking {
                    rows.push(PerformanceRankingRow {
                        fetched_at,
                        user_id: entry.user_id as i64,
                        user_name: entry.username.into_string(),
                        mode,
                        rank: entry.statistics.clone().unwrap().global_rank.unwrap() as i32,
                        pp: entry.statistics.unwrap().pp as f64,
                        country_code: entry.country_code.into_string(),
                    });
                }

                Ok::<_, eyre::Report>((page, mode, rows))
            })
            .buffer_unordered(MAX_CONCURRENT)
            .try_fold(
                Vec::new(),
                |mut all_rows, (page, mode, mut rows)| async move {
                    let page_rows = rows.len();

                    all_rows.append(&mut rows);

                    tracing::info!(?mode, page, page_rows, "Fetched ranking page");

                    Ok(all_rows)
                },
            )
            .await?;
        let stored = rows.len();

        // Single database insertion after all pages have been fetched.
        self.database.insert_performance_rankings(&rows).await?;
        tracing::info!(variant, stored, "Ranking fetch complete");

        Ok(stored)
    }
}

pub async fn handle_root(
    State(server): State<SharedServer>,
    Query(query): Query<RankingQuery>,
) -> Result<Json<RankingResponse>, (StatusCode, String)> {
    if let Err(v) = resolve_variant(&query.variant) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{v}: Unknown ranking variant: {}", query.variant),
        ));
    }

    match server.fetch_and_store(&query.variant).await {
        Ok(stored) => Ok(Json(RankingResponse {
            variant: query.variant,
            stored,
        })),
        Err(error) => {
            tracing::error!(?error, variant = %query.variant, "Ranking fetch failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
        }
    }
}
