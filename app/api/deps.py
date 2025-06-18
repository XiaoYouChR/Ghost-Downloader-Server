from fastapi import Request

def get_middleware(request: Request):
    return request.app.state.middleware

def get_core_engine(request: Request):
    return request.app.state.coreEngine

def get_plugin_service(request: Request):
    return request.app.state.pluginService

def get_config_service(request: Request):
    return request.app.state.configService