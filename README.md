# Consensus Protocol Simulation in OCaml

This is the reference code for my capstone project: modelling distributed consensus protocols using a modular simulation framework. The simulator code can be found in `sim.ml`.

This repository contains a (simplified) sample implementation of the SBAC protocol (`sbac.ml`, running (simplified) PBFT within the shards. The PBFT code is implemented as a standalone module (`pbft.ml`), which means it can be tested independently of SBAC. It is by no means a completed project, there is still much to improve!

## Usage

Open `executable.ml` and paste the first two lines (the REPL directives) into your OCaml REPL. The `executable.ml` file contains two examples:

1. Generating a random schedule
2. Manually adding conflicting transactions to see the result

The examples contain instructional comments, but essentially you'd just need to copy and paste some of the lines into your REPL again.
