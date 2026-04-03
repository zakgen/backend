DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chat_messages_intent_check'
    ) THEN
        ALTER TABLE chat_messages
        DROP CONSTRAINT chat_messages_intent_check;
    END IF;
END $$;

ALTER TABLE chat_messages
ADD CONSTRAINT chat_messages_intent_check CHECK (
    intent IN (
        'livraison',
        'prix',
        'disponibilite',
        'retour',
        'paiement',
        'infos_produit',
        'infos_boutique',
        'autre'
    )
);
