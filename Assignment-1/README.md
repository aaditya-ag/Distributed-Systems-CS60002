# How to setup the python dependencies?

Run the following commands in the terminal.

- To setup the virtual environment:
```
python3 -m venv env
```

- Activate the environment:
```
source env/bin/activate
```

- Install dependencies:
```
pip install -r requirements.txt
```

# How to setup the database?

- We will be using PostgreSQL for our project. Hence we are following [this link](https://www.postgresql.org/download/linux/ubuntu/) for setting up our database. Please follow the OS specific instructions for setting it up in systems other than linux.


- For Ubuntu Copy paste this script below
```
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql
```

- Now create a database by logging into your postgres client using the following commands
```
sudo -i -u postgres
```

- Now you will be interacting as postgres@server. Now enter psql in the CLI
```
psql
```

- Now you will be inside the psql command line utility. Here you can create a database as follows:
```
CREATE DATABASE "distributed_queue";
```

- Also change the password of user 'postgres' to get the password for accessing the database
```
ALTER USER postgres WITH PASSWORD 'admin';
``` 

- Keep pressing Ctrl^D to exit out of the terminal. Now we are good to go forward to make the app live.


# How to make the project live and running in your system?

- First Run the following commands to create the necessary tables in the database
```
flask db init
flask db migrate
flask db upgrade
```

- Run the app
```
flask run
```

### How to run the tests?

- For producer
```
python producer.py <broker_url> <File the producer reads from> 
```
Example:
```
python producer.py http://127.0.0.1:5000 ../test_asgn1/producer_1.txt 
```

- For consumer
```
python consumer.py <broker_url> <outfile_prefix> <topics_to_register> 
```
Example:
```
python consumer.py http://127.0.0.1:5000 consumer_1 T-1 T-2 T-3 
```