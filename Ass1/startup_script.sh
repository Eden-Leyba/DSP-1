sudo rm /home/ec2-user/.aws/labsuser.pem
sudo mv /home/ec2-user/credentials /home/ec2-user/.aws/credentials
aws s3 cp s3://$1/$2 /home/ec2-user/app.jar
cd /home/ec2-user
sudo yum install -y java-25-amazon-corretto-headless
nohup java -Xmx2g -Xms1g -jar app.jar > app.log 2>&1 &
echo "DONE STARTING APP"