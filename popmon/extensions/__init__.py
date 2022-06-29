from popmon.extensions.profile_diptest import Diptest

extensions = [Diptest()]
for extension in extensions:
    extension.check()
