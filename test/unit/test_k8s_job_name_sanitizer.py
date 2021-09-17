import re
from metaflow.plugins.aws.eks.kubernetes import generate_rfc1123_name

rfc1123 = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?$')

def test_job_name_santitizer():
    # Basic name
    assert rfc1123.match(generate_rfc1123_name('HelloFlow', '1', 'end', '321', '1'))

    # Step name ends with _
    assert rfc1123.match(generate_rfc1123_name('HelloFlow', '1', '_end', '321', '1'))

    # Step name starts and ends with _
    assert rfc1123.match(generate_rfc1123_name('HelloFlow', '1', '_end_', '321', '1'))

    # Flow name ends with _
    assert rfc1123.match(generate_rfc1123_name('HelloFlow_', '1', 'end', '321', '1'))

    # Same flow name, different case must produce different job names
    assert generate_rfc1123_name('Helloflow', '1', 'end', '321', '1') != generate_rfc1123_name('HelloFlow', '1', 'end', '321', '1')

    # Very long step name should be fine
    assert rfc1123.match(generate_rfc1123_name('Helloflow', '1', 'end'*50, '321', '1'))

    # Very long run id should be fine too
    assert rfc1123.match(generate_rfc1123_name('Helloflow', '1'*100, 'end', '321', '1'))