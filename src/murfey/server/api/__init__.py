from fastapi.templating import Jinja2Templates

from murfey.server import template_files

templates = Jinja2Templates(template_files)
