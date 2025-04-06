#!/bin/bash
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
export PYENV_VERSION=dev
/Users/admin-wana/.pyenv/shims/python3 /Users/admin-wana/Projects/finance-tweets-builder/main.py
