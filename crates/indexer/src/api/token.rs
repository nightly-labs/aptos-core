use std::sync::Arc;

use aptos_api::accept_type::AcceptType;
use aptos_api::failpoint::fail_point_poem;
use aptos_api::response::{BasicResponse, BasicResponseStatus};
use aptos_api::{AcceptType, BasicResponse, BasicResultWith404, Context};
use aptos_api_types::{Address, MoveStruct, U64};
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use poem_openapi::param::Path;
use poem_openapi::OpenApi;
use poem_openapi::{param::Query, payload::Json};

use crate::schema::current_token_ownerships;
use crate::{
    database::PgPoolConnection, models::token_models::token_ownerships::CurrentTokenOwnership,
};

pub struct TokenAPI {
    pub context: Arc<Context>,
    pub conn: PgPoolConnection,
}

pub struct TokenData {
    token_id: MoveStruct,
    amount: U64,
    token_properties: MoveStruct,
}

#[OpenAPI]
impl TokenAPI {
    #[oai(
        path = "/accounts/:address/tokens",
        method = "get",
        operation_id = "get_account_tokens",
        tag = "ApiTags::Tokens"
    )]
    async fn get_user_tokens(
        &self,
        accept_type: AcceptType,
        user_address: Path<Address>,
        ledger_version: Query<Option<U64>>,
    ) -> BasicResultWith404<Vec<TokenData>> {
        fail_point_poem("endpoint_get_account_resources")?;
        self.context
            .check_api_output_enabled("Get user tokens", &accept_type);
        let tokens = current_token_ownerships::table
            .filter(current_token_ownerships::owner_address.eq(user_address.0.inner().to_hex()))
            .load::<CurrentTokenOwnership>(&mut self.conn)
            .optional()
            .unwrap()
            .unwrap();
        match accept_type {
            AcceptType::Json => {
                BasicResponse::try_from_json((tokens, ledger_version.0, BasicResponseStatus::Ok))
            }
            AcceptType::Bcs => {
                BasicResponse::try_from_bcs((tokens, ledger_version.0, BasicResponseStatus::Ok))
            }
        }
    }
}
