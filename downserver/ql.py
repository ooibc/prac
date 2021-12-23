import random
import math
import numpy as np
import envs

from learn import Learner

# Q-Table q-learn agent.
class QT_Learner(Learner):
    def __init__(self, action_space, state_count, lr = 0.02, ga = 0.98):
        self.q = np.zeros((state_count, action_space.n))
        self.action_space = action_space
        self.lr = lr
        self.ga = ga

    def get(self, s, a):
        return self.q[s][a]

    def put(self, s, a, q_):
        self.q[s][a] = q_

    def max_q(self, s):
        max_qv, max_action = -math.inf, 0
        for a in range(self.action_space.n):
            r = self.get(s, a)
            if r > max_qv:
                max_qv, max_action = r, a
        return max_action, max_qv

    def choose_action(self, s, eps):
        if random.random() < eps:
            return self.action_space.sample()
        a, _ = self.max_q(s)
        return a

    def update_transition(self, s, a, r, s_t, done):
        q = (1 - self.lr) * self.get(s, a) + self.lr * (r + self.ga * self.max_q(s_t)[1] * done)
        self.put(s, a, q)

    # Sacrifice the exploitation and have more exploration for training.
    def train_next_step(self, env, s, eps):
        a = self.choose_action(s, eps)
        s_, r, done, _ = env.step(a)
        self.update_transition(s, a, r, s_, 0. if done else 1.)
        return s_, r, done

# 0 for next, 1 for stop.
# 16: start -> 2^0 -> ... -> 2^ 13 -> end.
def init(level):
    global env, model
    trans = np.zeros((16, 2))
    for i in range(15):
        trans[i][0] = 15
        trans[i][1] = i+1
    env = envs.QT_env(0, [0, 1], trans, level)
    model = QT_Learner(env, 2)
    return env, model