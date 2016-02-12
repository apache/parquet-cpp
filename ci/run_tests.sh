if [ $TRAVIS_OS_NAME == "linux" ]; then
  valgrind --tool=memcheck --leak-check=yes --error-exitcode=1 ctest;
else
  ctest;
fi
