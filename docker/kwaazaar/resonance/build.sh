echo "Cloning git repo..."
./gitpull.sh
echo "Removing old db-folder..."
sudo rm -f -r db/
echo "Building docker images..."
docker build . -t kwaazaar/resonance

