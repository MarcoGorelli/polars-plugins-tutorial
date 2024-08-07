# 14. Publishing your plugin to PyPI and becoming famous

Here are the steps you should follow:

1. publish plugin to PyPI
2. ???
3. profit

This section deals with step 1, and assumes your project live on GitHub.

## Set up trusted publishing

If you followed the [Prerequisites] steps, you should have `.github/workflows/publish_to_pypi.yml`,
`Makefile`, and `requirements.txt` files. If not, go back and follow the cookiecutter step.

Next, set up an account on Pypi.org, can't do much without that.

Third, on PyPI, you'll want to (note: this is taken almost verbatim from [PyPA](https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/#configuring-trusted-publishing)):

  1. Go to https://pypi.org/manage/account/publishing/.
  2. Fill in the name you wish to publish your new PyPI project under (the name value in your pyproject.toml), the GitHub repository owner’s name (org or user), and repository name, and the name of the release workflow file under the .github/ folder, see Creating a workflow definition. Finally, add the name of the GitHub Environment (pypi) we’re going set up under your repository. Register the trusted publisher.

Finally, if you make a commit and tag it, and then push, then a release should be triggered! It will then be
available for install across different platforms, which would be really hard (impossible?) to do if you were building
the wheel manually and uploading to PyPI yourself.

  [Prerequisites]: ../prerequisites/