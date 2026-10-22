@echo off
echo Stopping T16O Exchange Guide Workers...
taskkill /FI "WINDOWTITLE eq T16O-*" /F 2>nul
echo Done.
