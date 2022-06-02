import pytest

from popmon.base.registry import Registry


def test_registery():
    MyRegistry = Registry()

    @MyRegistry.register(key="example", description="hello world")
    def example_function(my_input):
        return 4 + my_input

    @MyRegistry.register(
        key=["coefficient", "p_value"],
        description=["phi_custom coefficient", "p-value for the phi_custom coeff."],
    )
    def phi_custom():
        return 0.25, 0.0001

    f = MyRegistry.get_func_by_name("example_function")

    # check that original function is intact
    assert f(1) == 5
    assert example_function(1) == 5

    # name should be the same the original function
    assert f.__name__ == "example_function"

    assert MyRegistry.get_keys() == ["example", "coefficient", "p_value"]
    assert MyRegistry.get_descriptions() == {
        "example": "hello world",
        "coefficient": "phi_custom coefficient",
        "p_value": "p-value for the phi_custom coeff.",
    }


def test_registry_properties():
    PropsRegistry = Registry()

    @PropsRegistry.register(key="hello", description="world", dim=3, htype="all")
    def my_func():
        return 0

    assert PropsRegistry.get_keys_by_dim_and_htype(dim=1, htype=None) == []
    assert PropsRegistry.get_keys_by_dim_and_htype(dim=3, htype="all") == ["hello"]


def test_registry_duplicate():
    DuplicatedRegistry = Registry()

    @DuplicatedRegistry.register(key="test", description="me")
    def func1():
        pass

    with pytest.raises(ValueError) as e:

        @DuplicatedRegistry.register(key="another", description="value")  # noqa: F811
        def func1():  # noqa: F811
            pass

    assert (
        e.value.args[0]
        == "A function with the name 'func1' has already been registered."
    )

    with pytest.raises(ValueError) as e:

        @DuplicatedRegistry.register(key="test", description="duplicate")
        def func2():
            pass

    assert e.value.args[0] == "Key 'test' has already been registered."


def test_registry_run():
    RunRegistry = Registry()

    @RunRegistry.register(key="key", description="run me", dim=1, htype="num")
    def func(arg1, arg2):
        return abs(arg1 - arg2)

    args = [1, 4]
    result = RunRegistry.run(args=args, dim=1, htype="num")
    assert result == {"key": 3}
