# fdp/tracing/jaeger_tracer.py
import structlog
from jaeger_client import Config
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
                'reporting_host': 'localhost',
                'reporting_port': '6831',
            },
            'logging': True,
        },
        service_name='FinDashPro',
        validate=True,
    )
    return config.initialize_tracer()
