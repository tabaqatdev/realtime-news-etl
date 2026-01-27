"""
GDELT Schema Definitions and Factory
Single source of truth for all GDELT column schemas
"""

from dataclasses import dataclass


@dataclass
class GDELTSchema:
    """Represents a GDELT data schema with column definitions"""

    columns: dict[str, str]
    essential_columns: list[str]

    def to_duckdb_dict(self, essential_only: bool = False) -> dict[str, str]:
        """Convert to DuckDB column definition dictionary"""
        if essential_only:
            return {col: self.columns[col] for col in self.essential_columns if col in self.columns}
        return self.columns.copy()

    def to_duckdb_string(self, essential_only: bool = False) -> str:
        """Convert to DuckDB column definition string for SQL"""
        cols = self.to_duckdb_dict(essential_only)
        return ", ".join([f"'{k}': '{v}'" for k, v in cols.items()])

    def get_select_clause(self, essential_only: bool = False) -> str:
        """Get column names as SELECT clause"""
        if essential_only:
            return ", ".join(self.essential_columns)
        return ", ".join(self.columns.keys())


class SchemaFactory:
    """Factory for GDELT schema definitions"""

    # Full GDELT Events schema (63 columns)
    _EVENT_COLUMNS = {
        "GLOBALEVENTID": "BIGINT",
        "SQLDATE": "INTEGER",
        "MonthYear": "INTEGER",
        "Year": "INTEGER",
        "FractionDate": "DOUBLE",
        "Actor1Code": "VARCHAR",
        "Actor1Name": "VARCHAR",
        "Actor1CountryCode": "VARCHAR",
        "Actor1KnownGroupCode": "VARCHAR",
        "Actor1EthnicCode": "VARCHAR",
        "Actor1Religion1Code": "VARCHAR",
        "Actor1Religion2Code": "VARCHAR",
        "Actor1Type1Code": "VARCHAR",
        "Actor1Type2Code": "VARCHAR",
        "Actor1Type3Code": "VARCHAR",
        "Actor2Code": "VARCHAR",
        "Actor2Name": "VARCHAR",
        "Actor2CountryCode": "VARCHAR",
        "Actor2KnownGroupCode": "VARCHAR",
        "Actor2EthnicCode": "VARCHAR",
        "Actor2Religion1Code": "VARCHAR",
        "Actor2Religion2Code": "VARCHAR",
        "Actor2Type1Code": "VARCHAR",
        "Actor2Type2Code": "VARCHAR",
        "Actor2Type3Code": "VARCHAR",
        "IsRootEvent": "INTEGER",
        "EventCode": "VARCHAR",
        "EventBaseCode": "VARCHAR",
        "EventRootCode": "VARCHAR",
        "QuadClass": "INTEGER",
        "GoldsteinScale": "DOUBLE",
        "NumMentions": "INTEGER",
        "NumSources": "INTEGER",
        "NumArticles": "INTEGER",
        "AvgTone": "DOUBLE",
        "Actor1Geo_Type": "INTEGER",
        "Actor1Geo_FullName": "VARCHAR",
        "Actor1Geo_CountryCode": "VARCHAR",
        "Actor1Geo_ADM1Code": "VARCHAR",
        "Actor1Geo_ADM2Code": "VARCHAR",
        "Actor1Geo_Lat": "DOUBLE",
        "Actor1Geo_Long": "DOUBLE",
        "Actor1Geo_FeatureID": "VARCHAR",
        "Actor2Geo_Type": "INTEGER",
        "Actor2Geo_FullName": "VARCHAR",
        "Actor2Geo_CountryCode": "VARCHAR",
        "Actor2Geo_ADM1Code": "VARCHAR",
        "Actor2Geo_ADM2Code": "VARCHAR",
        "Actor2Geo_Lat": "DOUBLE",
        "Actor2Geo_Long": "DOUBLE",
        "Actor2Geo_FeatureID": "VARCHAR",
        "ActionGeo_Type": "INTEGER",
        "ActionGeo_FullName": "VARCHAR",
        "ActionGeo_CountryCode": "VARCHAR",
        "ActionGeo_ADM1Code": "VARCHAR",
        "ActionGeo_ADM2Code": "VARCHAR",
        "ActionGeo_Lat": "DOUBLE",
        "ActionGeo_Long": "DOUBLE",
        "ActionGeo_FeatureID": "VARCHAR",
        "DATEADDED": "BIGINT",
        "SOURCEURL": "VARCHAR",
    }

    # Essential columns (drops 99%+ NULL columns for better performance)
    _EVENT_COLUMNS_ESSENTIAL = [
        # Core event identification
        "GLOBALEVENTID",
        "SQLDATE",
        "MonthYear",
        "Year",
        "FractionDate",
        # Actor 1 (essential only)
        "Actor1Code",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor1Type1Code",
        # Actor 2 (essential only)
        "Actor2Code",
        "Actor2Name",
        "Actor2CountryCode",
        "Actor2Type1Code",
        # Event classification
        "IsRootEvent",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        # Event metrics
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        # Actor 1 Geography
        "Actor1Geo_Type",
        "Actor1Geo_FullName",
        "Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code",
        "Actor1Geo_Lat",
        "Actor1Geo_Long",
        # Actor 2 Geography
        "Actor2Geo_Type",
        "Actor2Geo_FullName",
        "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code",
        "Actor2Geo_Lat",
        "Actor2Geo_Long",
        # Action Geography
        "ActionGeo_Type",
        "ActionGeo_FullName",
        "ActionGeo_CountryCode",
        "ActionGeo_ADM1Code",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        # Source
        "DATEADDED",
        "SOURCEURL",
    ]

    # GDELT Mentions schema
    _MENTIONS_COLUMNS = {
        "GLOBALEVENTID": "BIGINT",
        "EventTimeDate": "BIGINT",
        "MentionTimeDate": "BIGINT",
        "MentionType": "INTEGER",
        "MentionSourceName": "VARCHAR",
        "MentionIdentifier": "VARCHAR",
        "SentenceID": "INTEGER",
        "Actor1CharOffset": "INTEGER",
        "Actor2CharOffset": "INTEGER",
        "ActionCharOffset": "INTEGER",
        "InRawText": "INTEGER",
        "Confidence": "INTEGER",
        "MentionDocLen": "INTEGER",
        "MentionDocTone": "DOUBLE",
        "MentionDocTranslationInfo": "VARCHAR",
        "Extras": "VARCHAR",
    }

    @classmethod
    def get_event_schema(cls, essential_only: bool = False) -> GDELTSchema:
        """
        Get GDELT Events schema

        Args:
            essential_only: If True, only include essential columns (drops 99%+ NULL columns)

        Returns:
            GDELTSchema object with column definitions
        """
        return GDELTSchema(
            columns=cls._EVENT_COLUMNS,
            essential_columns=cls._EVENT_COLUMNS_ESSENTIAL
            if essential_only
            else list(cls._EVENT_COLUMNS.keys()),
        )

    @classmethod
    def get_mentions_schema(cls) -> GDELTSchema:
        """Get GDELT Mentions schema"""
        return GDELTSchema(
            columns=cls._MENTIONS_COLUMNS, essential_columns=list(cls._MENTIONS_COLUMNS.keys())
        )

    @classmethod
    def get_column_names(cls, data_type: str = "export", essential_only: bool = False) -> list[str]:
        """
        Get column names for a specific GDELT data type

        Args:
            data_type: One of "export" (events), "mentions"
            essential_only: If True, return only essential columns

        Returns:
            List of column names
        """
        if data_type == "export":
            if essential_only:
                return cls._EVENT_COLUMNS_ESSENTIAL
            return list(cls._EVENT_COLUMNS.keys())
        elif data_type == "mentions":
            return list(cls._MENTIONS_COLUMNS.keys())
        else:
            raise ValueError(f"Unknown data type: {data_type}")
