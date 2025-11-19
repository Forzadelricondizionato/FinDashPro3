@ECHO OFF

pushd %~dp0

set SOURCEDIR=.
set BUILDDIR=_build
set SPHINXPROJ=FinDashProMLMax

if "%1" == "" goto help
if "%1" == "livehtml" goto livehtml
if "%1" == "clean" goto clean

%SPHINXBUILD% >NUL 2>NUL
if errorlevel 9009 (
	echo.
	echo.Sphinx non installato. Esegui:
	echo.   pip install -r docs/requirements.txt
	exit /b 1
)

%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%
goto end

:help
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS%

:livehtml
sphinx-autobuild %SOURCEDIR% %BUILDDIR%/html --open-browser --delay=0
goto end

:clean
rm -rf %BUILDDIR%/*
goto end

:end
popd

