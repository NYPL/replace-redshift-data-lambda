#!/bin/zsh

rm -f -r ./package
rm -f deployment-package.zip
pip3.9 install --target ./package -r requirements.txt
cd package
zip -r ../deployment-package.zip .
cd ..
zip deployment-package.zip lambda_function.py