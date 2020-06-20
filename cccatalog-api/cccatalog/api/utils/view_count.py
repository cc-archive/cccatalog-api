def _get_user_ip(request):
    """
    Read request headers to find the correct IP address.

    It is assumed that X-Forwarded-For has been sanitized by the load balancer
    and thus cannot be rewritten by malicious users.

    :param request: A Django request object.
    :return: An IP address.
    """
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip
