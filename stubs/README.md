Metaflow stubs generator

To run:
`rm -rf metaflow-stubs/ && python -c "from stub_generator import StubGenerator; gen = StubGenerator('./metaflow-stubs'); gen.write_out()"`

You should then be able to install the resulting package et voila.
