# -- Project information -----------------------------------------------------

project = 'Replay'
copyright = '2024, Ivan A. Kudriavtsev'
author = 'Ivan A. Kudriavtsev'

release = '0.1.0'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.inheritance_diagram',
    'sphinx.ext.autosectionlabel',
    'sphinx_rtd_theme',
]

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
html_static_path = ['_static']

autosummary_generate = True
autosummary_imported_members = True
templates_path = ['_templates']
exclude_patterns = []
html_theme = 'sphinx_rtd_theme'
