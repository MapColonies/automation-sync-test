"""
This module wrap the relevant api access to trigger sync process via NIFI api
"""
from sync_tester.configuration import config
from mc_automation_tools import base_requests, common

sync_trigger_body = {
    'resourceId': 'fill resource id',
    'version': 'fill version',
    'productType': config.ProductType.orthophoto_history.value,
    'operation': config.SyncOperationType.ADD.value,
    'layerRelativePath': 'path/to/layer'
}


def send_sync_request(body_request: dict) -> dict:
    """
    This method will trigger sync process
    :param body_request: {
                            'resourceId',
                            'version',
                            'productType',
                            'operation',
                            'layerRelativePath'}
    :return: dict -> {status code: int, msg: response message}
    """

    # todo -> may be deprecated on future -> the executors will be responsible to send the full body
    # if body_request['productType'] != config.ProductType.orthophoto_history.value:
    #     body_request['layerRelativePath'] = f'{body_request["resourceId"]}/{body_request["version"]}'
    # else:
    #     body_request['layerRelativePath'] = f'{body_request["resourceId"]}/{body_request["version"]}/{body_request["productType"]}'

    # url = common.combine_url(config.TRIGGER_NIFI_ROUTE_CORE_A, config.NIFI_SYNC_TRIGGER_API_CORE_A)
    # todo insert to mc-automation-tools

    import urllib.parse
    url = urllib.parse.urljoin(config.TRIGGER_NIFI_ROUTE_CORE_A, config.NIFI_SYNC_TRIGGER_API_CORE_A)
    resp = base_requests.send_post_request(url, body_request)
    s_code, msg = common.response_parser(resp)
    return {'status_code': s_code, 'msg': msg}


