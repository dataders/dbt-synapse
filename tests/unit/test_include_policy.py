def test_render_include_policy(adapter):
    relation = adapter.Relation.create(
        database="my_database", schema="my_schema", identifier="my_view", type="view"
    )
    assert relation.render() == '"my_schema"."my_view"'
