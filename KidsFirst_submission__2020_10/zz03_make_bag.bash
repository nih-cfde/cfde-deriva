#!/bin/bash

bdbag --quiet --archiver tgz KF_C2M2_submission

bdbag --quiet --revert KF_C2M2_submission
