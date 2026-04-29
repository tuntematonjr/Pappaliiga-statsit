#!/bin/bash

# Siirrytään kansioon, jossa tämä skripti sijaitsee
cd "$(dirname "$0")" || exit 1

# Aktivoidaan virtuaaliympäristö (suhteellinen polku toimii nyt, koska cd tehtiin ylempänä)
source .venv/bin/activate

echo "Pysäytetään palvelu..."
sudo systemctl stop pappastatsit.service

echo "Haetaan päivitykset..."
git pull

echo "Käynnistetään palvelu..."
sudo systemctl start pappastatsit.service

echo "Päivitys valmis!"