use super::Conformance;
use serde::{Deserialize, Serialize};
use stac::Catalog;

/// The root landing page of a STAC API.
///
/// In a STAC API, the root endpoint (Landing Page) has the following characteristics:
///
///   - The returned JSON is a [STAC
///     Catalog](../stac-spec/catalog-spec/catalog-spec.md), and provides any number
///     of 'child' links to navigate to additional
///     [Catalog](../stac-spec/catalog-spec/catalog-spec.md),
///     [Collection](../stac-spec/collection-spec/README.md), and
///     [Item](../stac-spec/item-spec/README.md) objects.
///   - The `links` attribute is part of a STAC Catalog, and provides a list of
///     relations to API endpoints. Some of these endpoints can exist on any path
///     (e.g., sub-catalogs) and some have a specified path (e.g., `/search`), so
///     the client must inspect the `rel` (relationship) to understand what
///     capabilities are offered at each location.
///   - The `conformsTo` section provides the capabilities of this service. This
///     is the field that indicates to clients that this is a STAC API and how to
///     access conformance classes, including this one. The relevant conformance
///     URIs are listed in each part of the API specification. If a conformance
///     URI is listed then the service must implement all of the required
///     capabilities.
#[derive(Debug, Serialize, Deserialize)]
pub struct Root {
    /// The [stac::Catalog].
    #[serde(flatten)]
    pub catalog: Catalog,

    /// Provides the capabilities of this service.
    ///
    /// This is the field that indicates to clients that this is a STAC API and
    /// how to access conformance classes, including this one. The relevant
    /// conformance URIs are listed in each part of the API specification. If a
    /// conformance URI is listed then the service must implement all of the
    /// required capabilities.
    ///
    /// Note the `conformsTo` array follows the same structure of the OGC API -
    /// Features [declaration of conformance
    /// classes](http://docs.opengeospatial.org/is/17-069r3/17-069r3.html#_declaration_of_conformance_classes),
    /// except it is part of the landing page instead of in the JSON response
    /// from the `/conformance` endpoint. This is different from how the OGC API
    /// advertises conformance, as STAC feels it is important for clients to
    /// understand conformance from a single request to the landing page.
    /// Implementers who implement the *OGC API - Features* and/or *STAC API -
    /// Features* conformance classes must also implement the `/conformance`
    /// endpoint.
    ///
    /// The scope of the conformance classes declared in the
    /// `conformsTo` field and the `/conformance` endpoint are limited to the
    /// STAC API Catalog that declares them. A STAC API Catalog may link to
    /// sub-catalogs within it via `child` links that declare different
    /// conformance classes. This is useful when an entire catalog cannot be
    /// searched against to support the *STAC API - Item Search* conformance
    /// class, perhaps because it uses multiple databases to store items, but
    /// sub-catalogs whose items are all in one database can support search.
    /// #[serde(rename = "conformsTo")]
    #[serde(flatten)]
    pub conformance: Conformance,
}
