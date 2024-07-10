"""Test Influx db parser"""

import unittest
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from oteapi_dlite.strategies.oceanlab_influx_parser import (
    InfluxParseParseConfig,
    InfluxParseStrategy,
    InfluxParseStrategyConfig,
    query_to_df,
)


class TestInfluxParseParseConfig(unittest.TestCase):
    """Test configuration"""

    def test_valid_config(self):
        """Test configuration validity"""
        config = InfluxParseParseConfig(
            id="test_id",
            label="test_label",
            resourceType="resource/url",
            downloadUrl="http://example.com",
            mediaType="application/json",
            storage_path="/path/to/storage",
            collection_id="test_collection_id",
            url="http://db.url",
            USER="test_user",
            PASSWORD="test_password",
            DATABASE="test_db",
            RETPOLICY="test_policy",
        )
        self.assertEqual(config.id, "test_id")
        self.assertEqual(config.label, "test_label")

    def test_invalid_config(self):
        """Test validation error"""
        with self.assertRaises(ValidationError):
            InfluxParseParseConfig(id=123)  # id should be a string or None


class TestInfluxParseStrategyConfig(unittest.TestCase):
    def test_valid_strategy_config(self):
        """Test config instance"""
        parse_config = InfluxParseParseConfig()
        strategy_config = InfluxParseStrategyConfig(
            parserType="influx/vnd.dlite-influx",
            entity="http://onto-ns.com/meta/0.4/dummy",
            configuration=parse_config,
        )
        self.assertIsInstance(
            strategy_config.configuration, InfluxParseParseConfig
        )


class TestInfluxParseStrategy(unittest.TestCase):
    """Test startegy initialize and get"""

    def test_initialize(self, mock_get_collection):
        """Test initialize function"""
        mock_collection = MagicMock()
        mock_collection.uuid = "test_uuid"
        mock_get_collection.return_value = mock_collection

        strategy = InfluxParseStrategy(
            parse_config=InfluxParseStrategyConfig(
                configuration=InfluxParseParseConfig()
            )
        )
        session_update = strategy.initialize()
        self.assertEqual(session_update.collection_id, "test_uuid")

    def test_get(self, mock_get_collection, mock_query_to_df, mock_get_meta):
        """test get function"""
        mock_collection = MagicMock()
        mock_collection.uuid = "test_uuid"
        mock_get_collection.return_value = mock_collection

        mock_df = MagicMock()
        mock_query_to_df.return_value = mock_df

        mock_meta = MagicMock()
        mock_inst = MagicMock()
        mock_meta.return_value = mock_inst
        mock_get_meta.return_value = mock_meta

        strategy = InfluxParseStrategy(
            parse_config=InfluxParseStrategyConfig(
                configuration=InfluxParseParseConfig(
                    url="http://db.url",
                    USER="test_user",
                    PASSWORD="test_password",
                    DATABASE="test_db",
                    RETPOLICY="test_policy",
                    storage_path="/path/to/storage|another/path",
                    label="test_label",
                )
            )
        )
        session_update = strategy.get()
        self.assertEqual(session_update.collection_id, "test_uuid")
        self.assertEqual(session_update.label, "test_label")
        mock_get_collection.assert_called()
        mock_query_to_df.assert_called()
        mock_get_meta.assert_called()

    @patch("influxdb_client.InfluxDBClient")
    def test_query_to_df(self, mock_influxdb_client):
        """Test query to df"""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mock_influxdb_client.return_value.__enter__.return_value = mock_client

        mock_df = MagicMock()
        mock_query_api.query_data_frame.return_value = mock_df

        result = query_to_df(
            "test_query", "http://db.url", "test_user", "test_password"
        )
        self.assertEqual(result, mock_df)
        mock_influxdb_client.assert_called_once_with(
            url="http://db.url", token="test_user:test_password"
        )
