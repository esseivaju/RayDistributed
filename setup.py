from setuptools import setup, find_packages

setup(
    name='RayDistributed',
    version="0.1.0",
    author='Julien Esseiva',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': ['RayDistributed=raydistributed.scripts.ray_distributed:main', "raysync=raydistributed.scripts.raysync:main"]
    },
    install_requires=[
        'ray',
        'click'
    ]
)