import unittest
from types import SimpleNamespace

from core.offer_identity import (
    build_offer_id_lookup_maps,
    flight_offer_identity_key,
    resolve_offer_id,
)


class OfferIdentityTests(unittest.TestCase):
    def setUp(self):
        self.row = SimpleNamespace(
            id=101,
            airline="BG",
            origin="DAC",
            destination="CGP",
            departure="2026-03-27T06:15:00",
            flight_number="343",
            cabin="Economy",
            fare_basis="E",
            brand="AMYBD_BD",
        )
        self.lookup_maps = build_offer_id_lookup_maps([self.row])

    def test_resolve_offer_id_matches_exact_identity(self):
        identity = flight_offer_identity_key(
            airline="BG",
            origin="DAC",
            destination="CGP",
            departure="2026-03-27T06:15:00",
            flight_number="343",
            cabin="Economy",
            fare_basis="E",
            brand="AMYBD_BD",
        )

        row_id, match_mode = resolve_offer_id(identity, self.lookup_maps)

        self.assertEqual(101, row_id)
        self.assertEqual("exact", match_mode)

    def test_resolve_offer_id_falls_back_when_brand_drifts(self):
        identity = flight_offer_identity_key(
            airline="BG",
            origin="DAC",
            destination="CGP",
            departure="2026-03-27T06:15:00",
            flight_number="343",
            cabin="Economy",
            fare_basis="E",
            brand="SABRE_BRAND",
        )

        row_id, match_mode = resolve_offer_id(identity, self.lookup_maps)

        self.assertEqual(101, row_id)
        self.assertEqual("no_brand", match_mode)

    def test_resolve_offer_id_falls_back_when_fare_basis_drifts(self):
        identity = flight_offer_identity_key(
            airline="BG",
            origin="DAC",
            destination="CGP",
            departure="2026-03-27T06:15:00",
            flight_number="343",
            cabin="Economy",
            fare_basis="Y",
            brand="AMYBD_BD",
        )

        row_id, match_mode = resolve_offer_id(identity, self.lookup_maps)

        self.assertEqual(101, row_id)
        self.assertEqual("no_fare_basis", match_mode)

    def test_resolve_offer_id_falls_back_to_core_identity(self):
        identity = flight_offer_identity_key(
            airline="BG",
            origin="DAC",
            destination="CGP",
            departure="2026-03-27T06:15:00",
            flight_number="343",
            cabin="Economy",
            fare_basis="Y",
            brand="SABRE_BRAND",
        )

        row_id, match_mode = resolve_offer_id(identity, self.lookup_maps)

        self.assertEqual(101, row_id)
        self.assertEqual("core", match_mode)


if __name__ == "__main__":
    unittest.main()
