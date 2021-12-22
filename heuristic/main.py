import ql

Train_Round = 10000

mo = [ql.init(2), ql.init(3)]
eps = [0.3, 0.3]

def action(level):
    i = level - 2
    eps[i] = max(0.0, eps[i] -  0.3 / Train_Round)
    if eps[i] > 0:
        mo[i][0].state, r, done = mo[i][1].train_next_step(mo[i][0], mo[i][0].state, eps[i])
    else:
        mo[i][0].state, r, done = mo[i][1].next_step(mo[i][0], mo[i][0].state)

    if done:
        mo[i][0].reset()
        return 0
    else:
        return 2 ** max(mo[i][0].state-2, 0)

def report(level, H, success, delta):
