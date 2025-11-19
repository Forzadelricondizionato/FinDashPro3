import os
import sys
sys.path.insert(0, os.path.abspath('..'))

project = 'FinDashPro ML-Max'
copyright = '2024, FinDashPro Team'
author = 'FinDashPro Team'
release = '3.1.4'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx_copybutton',
    'myst_parser',
    'sphinx_reredirects'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'sphinx_book_theme'
html_title = "FinDashPro ML-Max 3.1.4"
html_theme_options = {
    "repository_url": "https://github.com/findashpro/findashpro",
    "use_repository_button": True,
    "use_download_button": True,
    "use_fullscreen_button": True,
    "home_page_in_toc": True,
    "show_navbar_depth": 2,
    "show_toc_level": 2,
    "icon_links": [
        {
            "name": "Docker Hub",
            "url": "https://hub.docker.com/r/findashpro/findashpro",
            "icon": "fab fa-docker"
        },
        {
            "name": "Telegram",
            "url": "https://t.me/findashpro",
            "icon": "fab fa-telegram"
        }
    ]
}

html_static_path = ['_static']
html_css_files = ['custom.css']

autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

autosummary_generate = True
autosummary_imported_members = True

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

intersphinx_mapping = {
    'python': ('https://docs.python.org/3.11', None),
    'pandas': ('https://pandas.pydata.org/docs', None),
    'numpy': ('https://numpy.org/doc/stable', None),
    'sklearn': ('https://scikit-learn.org/stable', None)
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "html_image",
    "replacements",
    "smartquotes",
    "substitution"
]

redirects = {
    "installation": "setup.html",
    "quickstart": "tutorial.html"
}

def setup(app):
    app.add_css_file('custom.css')
