from jaeger_client import Config
import structlog
import opentracing

logger = structlog.get_logger()

def init_tracer():
    config = Config(
        config={
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'local_agent': {
                'reporting_host': 'jaeger',
                'reporting_port': 6831,
            },
            'logging': True,
        },
        service_name='fdp-orchestrator',
        validate=True,
    )
    return config.initialize_tracer()

tracer = init_tracer()

def trace_function(func_name):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            with tracer.start_active_span(func_name) as scope:
                scope.span.set_tag('component', 'fdp')
                try:
                    result = await func(*args, **kwargs)
                    scope.span.set_tag('error', False)
                    return result
                except Exception as e:
                    scope.span.set_tag('error', True)
                    scope.span.log_kv({'error': str(e)})
                    raise
        return wrapper
    return decorator

