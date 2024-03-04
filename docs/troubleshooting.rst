Troubleshooting
===============

OOM in your model
==============================================

Models often consume a significant amount of memory, making processes that utilize them susceptible to OOM killer termination when there is insufficient RAM.

This may manifest in less obvious ways, particularly when using Kubernetes. The following events may indicate OOM killer activation:
  - Flow Aqueduct stopping with the message `The process %s for %s handler is dead`;
  - Decrease in RAM consumption;
  - OOM killer activations visible in the kernel logs (dmesg). For example:

.. code-block:: bash

     [22484121.107687] CPU: 30 PID: 10608 Comm: python3 Tainted: P           OE     4.19.0-0.bpo.8-amd64 #1 Debian 4.19.98-1~bpo9+1
     [22484121.108329] [  10222]     0 10222   755450   134545  1970176        0           973 python3
     [22484121.108333] [  10325]     0 10325     4085     2741    77824        0           973 python3
     [22484121.108335] [  10330]     0 10330   800299   125661  1642496        0           973 python3
     [22484121.108337] [  10331]     0 10331   808101   133954  1699840        0           973 python3
     [22484121.108338] [  10332]     0 10332   795623   121491  1597440        0           973 python3
     [22484121.108341] [  10333]     0 10333   765984    91853  1363968        0           973 python3
     [22484121.108343] [  10336]     0 10336   710940    55090  1011712        0           973 python3
     [22484121.108344] [  10337]     0 10337   712501    58063  1036288        0           973 python3
     [22484121.108357] Memory cgroup out of memory: Kill process 10222 (python3) score 1038 or sacrifice child
     [22484121.108898] Killed process 10331 (python3) total-vm:3226164kB, anon-rss:509576kB, file-rss:20128kB, shmem-rss:44kB
     [22484121.247007] oom_reaper: reaped process 10334 (python3), now anon-rss:0kB, file-rss:77084kB, shmem-rss:31044kB
     [22484121.308216] oom_reaper: reaped process 10335 (python3), now anon-rss:0kB, file-rss:77084kB, shmem-rss:35204kB

Kubernetes case:
==============================================
In the Kubernetes infrastructure, when node resources are insufficient,
the OOM killer mechanism is triggered, which may terminate one of the processes than use models.
This leads to an inconsistent state of the pod.

Ensure that your Kubernetes version is 1.28 or higher.

Starting from version 1.28, Kubernetes supports the `memory.oom.group` https://github.com/torvalds/linux/blob/3c4aa44343777844e425c28f1427127f3e55826f/Documentation/admin-guide/cgroup-v2.rst?plain=1#L1280
setting. All processes within the pod are placed in a single cgroup,
allowing the termination of the entire pod upon OOM killer activation,
preventing an inconsistent pod state.

- Upgrade your Kubernetes cluster to version 1.28 or higher.
- Confirm that this pull request https://github.com/kubernetes/kubernetes/pull/117793 has been incorporated into your Kubernetes installation.



