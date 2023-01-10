#!/bin/zsh

pip install --target ./package -r requirements.txt
cd package
zip -r ../deployment-package.zip .
cd ..
zip deployment-package.zip lambda_function.py
zip deployment-package.zip helpers
zip deployment-package.zip helpers/kms_helper.py
zip deployment-package.zip helpers/log_helper.py