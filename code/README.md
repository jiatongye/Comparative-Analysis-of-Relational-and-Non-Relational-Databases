install mongo
brew tap mongodb/brew
brew install mongodb-community@6.0

start service
brew services start mongodb/brew/mongodb-community@6.0

run mongo
mongosh

run the python script data_load_mongoDB.py
pip3 pymongo
when installing pymongo, if you receive an error, try to open a virtual environment
python3 -m venv venv
source venv/bin/activate

