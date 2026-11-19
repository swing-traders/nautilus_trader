// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{Router, http::StatusCode, routing::any};
use nautilus_bybit::http::client::BybitRawHttpClient;

#[tokio::test]
async fn test_order_posts_are_single_shot_and_gets_retry() {
    for (post, expected) in [(false, 3), (true, 1)] {
        let count = Arc::new(AtomicUsize::new(0));
        let received = Arc::clone(&count);
        let router = Router::new().route(
            "/{*path}",
            any(move || {
                received.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::INTERNAL_SERVER_ERROR }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = BybitRawHttpClient::with_credentials(
            "test-key".into(),
            "test-secret".into(),
            Some(format!("http://{address}")),
            5,
            2,
            1,
            1,
            5_000,
            None,
        )
        .unwrap();
        if post {
            assert!(client.place_order(&serde_json::json!({})).await.is_err());
        } else {
            assert!(client.get_server_time().await.is_err());
        }
        server.abort();
        assert_eq!(count.load(Ordering::SeqCst), expected);
    }
}
