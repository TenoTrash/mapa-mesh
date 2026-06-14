#!/bin/bash
# install.sh - Setup de entorno para mapa-mesh
# Ejecutar con: bash install.sh

set -e

echo "==> Creando entorno virtual..."
python3 -m venv venv

echo "==> Activando entorno..."
source venv/bin/activate

echo "==> Instalando dependencias..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "✓ Instalación completa."
echo ""
echo "Para ejecutar el mapa:"
echo "  source venv/bin/activate"
echo "  python mapa_mesh.py"
echo ""
echo "Luego abrir en el navegador: http://127.0.0.1:8080"
