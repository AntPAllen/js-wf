"""Audit complete native profiler counts; only definite CAS rejection is eligible."""


def audit_profile(operations):
    if not isinstance(operations, dict) or not operations:
        raise ValueError('missing native operation profile')
    errors = []
    unexpected = []
    for operation, value in operations.items():
        for field in ('calls', 'errors', 'total_ns'):
            if type(value.get(field)) is not int or value[field] < 0:
                raise ValueError('invalid profile counter')
        if value['errors'] > value['calls']:
            raise ValueError('errors exceed calls')
        kinds = value.get('error_kinds')
        if kinds is not None:
            if not isinstance(kinds, dict) or any(type(n) is not int or n < 0 for n in kinds.values()):
                raise ValueError('invalid profile error classes')
            if sum(kinds.values()) != value['errors']:
                raise ValueError('error classes do not account for all errors')
        if not value['errors']:
            continue
        row = dict(operation=operation, calls=value['calls'], errors=value['errors'], error_kinds=kinds)
        errors.append(row)
        # No inference from historical counts, wrapped sentinels, or other
        # operations. Every eligible error must be a definite native CAS reject.
        if operation not in ('CASRoot', 'CASBlob') or kinds != {'conflict': value['errors']}:
            unexpected.append(row)
    return dict(errors=errors, unexpected=unexpected, accepted=not unexpected)
