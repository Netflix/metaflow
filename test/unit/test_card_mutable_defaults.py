from metaflow.plugins.cards.card_modules.basic import (
    SectionComponent,
    PageComponent,
    DagComponent,
    ArtifactsComponent,
    TableComponent,
)


class TestCardMutableDefaults:
    """Verify that card components do not share mutable default arguments."""

    def test_section_component_no_shared_contents(self):
        """Two SectionComponents created without args get independent lists."""
        a = SectionComponent()
        b = SectionComponent()
        assert a._contents is not b._contents

    def test_page_component_no_shared_contents(self):
        """Two PageComponents created without args get independent lists."""
        a = PageComponent()
        b = PageComponent()
        assert a._contents is not b._contents

    def test_dag_component_no_shared_data(self):
        """Two DagComponents created without args get independent dicts."""
        a = DagComponent()
        b = DagComponent()
        assert a._data is not b._data

    def test_artifacts_component_no_shared_data(self):
        """Two ArtifactsComponents created without args get independent dicts."""
        a = ArtifactsComponent()
        b = ArtifactsComponent()
        assert a._data is not b._data

    def test_table_component_no_shared_headers(self):
        """Two TableComponents created without args get independent headers."""
        a = TableComponent()
        b = TableComponent()
        assert a._headers is not b._headers

    def test_table_component_no_shared_data(self):
        """Two TableComponents created without args get independent data."""
        a = TableComponent()
        b = TableComponent()
        assert a._data is not b._data

    def test_explicit_contents_preserved(self):
        """Passing explicit contents uses the provided list."""
        custom = ["item1", "item2"]
        comp = SectionComponent(contents=custom)
        assert comp._contents is custom

    def test_explicit_data_preserved(self):
        """Passing explicit data uses the provided dict."""
        custom = {"key": "value"}
        comp = DagComponent(data=custom)
        assert comp._data is custom

    def test_table_explicit_headers_preserved(self):
        """Passing explicit headers to TableComponent uses the provided list."""
        custom_headers = ["col1", "col2"]
        custom_data = [["a", "b"]]
        comp = TableComponent(headers=custom_headers, data=custom_data)
        assert comp._headers is custom_headers
        assert comp._data is custom_data

    def test_default_contents_is_empty_list(self):
        """Default contents is an empty list, not None."""
        comp = SectionComponent()
        assert comp._contents == []
        assert isinstance(comp._contents, list)

    def test_default_data_is_empty_dict(self):
        """Default data is an empty dict, not None."""
        comp = DagComponent()
        assert comp._data == {}
        assert isinstance(comp._data, dict)

    def test_table_default_headers_is_empty_list(self):
        """Default headers is an empty list, not None."""
        comp = TableComponent()
        assert comp._headers == []
        assert isinstance(comp._headers, list)

    def test_mutation_does_not_leak_between_instances(self):
        """Mutating one instance's default does not affect another."""
        a = SectionComponent()
        a._contents.append("leaked")

        b = SectionComponent()
        assert b._contents == []

    def test_dict_mutation_does_not_leak_between_instances(self):
        """Mutating one instance's default dict does not affect another."""
        a = DagComponent()
        a._data["leaked"] = True

        b = DagComponent()
        assert b._data == {}

    def test_table_mutation_does_not_leak_between_instances(self):
        """Mutating one TableComponent's default does not affect another."""
        a = TableComponent()
        a._headers.append("leaked")

        b = TableComponent()
        assert b._headers == []
