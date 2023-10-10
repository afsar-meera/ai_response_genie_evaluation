def data_exist_check(data: dict, checklist: list, data_types=None):
    missing = []
    for i_, i in enumerate(checklist):
        if i not in data:
            missing.append(i)
            continue

        if data_types and type(data[i]) is not data_types[i_]:
            assert False, "Invalid Datatype for `%s`, Required %s" % (
                i,
                str(data_types[i_])
            )

    assert len(missing) == 0, "Missing required data points: %s" % ", ".join(missing)


if __name__ == "__main__":
    data_exist_check({'a': [], 'b': {}}, ['a', 'b'])
