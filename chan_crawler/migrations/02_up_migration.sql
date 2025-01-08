
CREATE TABLE IF NOT EXISTS posts (
    id BIGSERIAL PRIMARY KEY,
    post_number BIGINT NOT NULL,
    thread_number BIGINT NOT NULL,
    board TEXT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS posts_post_number_idx ON posts (post_number);
CREATE INDEX IF NOT EXISTS posts_thread_post_idx ON posts (thread_number, post_number);
CREATE INDEX IF NOT EXISTS posts_board_thread_post_idx ON posts (board, thread_number, post_number);

DELETE FROM posts a USING posts b
WHERE a.id > b.id 
AND a.board = b.board 
AND a.thread_number = b.thread_number 
AND a.post_number = b.post_number;

ALTER TABLE posts 
ADD CONSTRAINT posts_board_thread_post_unique 
UNIQUE (board, thread_number, post_number);